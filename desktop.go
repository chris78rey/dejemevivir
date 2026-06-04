package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

type uiToggle interface {
	Enable()
	Disable()
}

func runDesktopApp(user, pass, connStr, host string, port int, service, query, out string, timeout time.Duration) error {
	a := app.NewWithID("dejemevivir.exporter")
	w := a.NewWindow("Dejemevivir Exporter")
	w.Resize(fyne.NewSize(1100, 820))
	w.CenterOnScreen()

	defaults := exportDefaults{
		User:    user,
		Pass:    pass,
		ConnStr: connStr,
		Host:    host,
		Port:    port,
		Service: service,
		Query:   query,
		Out:     out,
		Timeout: timeout,
	}

	sqlPath := widget.NewEntry()
	sqlPath.SetPlaceHolder("Selecciona un archivo .sql")
	outPath := widget.NewEntry()
	outPath.SetPlaceHolder("Selecciona dónde guardar el .parquet")

	userEntry := widget.NewEntry()
	userEntry.SetText(defaults.User)
	passEntry := widget.NewPasswordEntry()
	passEntry.SetText(defaults.Pass)
	connEntry := widget.NewMultiLineEntry()
	connEntry.SetText(defaults.ConnStr)
	connEntry.SetPlaceHolder("Descriptor completo o deja vacío para usar host/puerto/servicio")
	hostEntry := widget.NewEntry()
	hostEntry.SetText(defaults.Host)
	portEntry := widget.NewEntry()
	portEntry.SetText(strconv.Itoa(defaults.Port))
	serviceEntry := widget.NewEntry()
	serviceEntry.SetText(defaults.Service)
	timeoutEntry := widget.NewEntry()
	timeoutEntry.SetText(defaults.Timeout.String())
	queryEntry := widget.NewMultiLineEntry()
	queryEntry.SetText(defaults.Query)
	queryEntry.Wrapping = fyne.TextWrapWord
	queryEntry.SetMinRowsVisible(12)

	statusLabel := widget.NewLabel("Listo para cargar un SQL y exportar")
	progress := widget.NewProgressBar()
	progress.Hide()
	progressLabel := widget.NewLabel("")
	progressLabel.Wrapping = fyne.TextWrapWord
	logBox := widget.NewMultiLineEntry()
	logBox.SetMinRowsVisible(10)
	logBox.SetText("Esperando acción.\n")
	logBox.Wrapping = fyne.TextWrapWord

	exportBtn := widget.NewButtonWithIcon("Ejecutar exportación", theme.MediaPlayIcon(), nil)
	openSqlBtn := widget.NewButtonWithIcon("Cargar SQL", theme.FolderOpenIcon(), nil)
	saveBtn := widget.NewButtonWithIcon("Elegir salida", theme.DocumentSaveIcon(), nil)
	openOutBtn := widget.NewButtonWithIcon("Abrir carpeta", theme.FolderOpenIcon(), nil)
	clearBtn := widget.NewButton("Limpiar logs", func() {
		logBox.SetText("")
	})

	var running bool
	var lastResult *exportResult

	appendLog := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		fyne.Do(func() {
			logBox.SetText(logBox.Text + msg)
		})
	}

	setRunning := func(active bool) {
		running = active
		if active {
			exportBtn.Disable()
			progress.Show()
		} else {
			exportBtn.Enable()
			progress.Hide()
		}
	}

	setFieldState := func(enabled bool) {
		fields := []uiToggle{userEntry, passEntry, connEntry, hostEntry, portEntry, serviceEntry, timeoutEntry, queryEntry, sqlPath, outPath, openSqlBtn, saveBtn}
		for _, field := range fields {
			if enabled {
				field.Enable()
			} else {
				field.Disable()
			}
		}
	}

	updateProgress := func(phase string, percent int, message string) {
		fyne.Do(func() {
			progress.SetValue(float64(clampProgress(percent)) / 100.0)
			progressLabel.SetText(message)
			statusLabel.SetText(message)
		})
		switch phase {
		case "connecting":
			appendLog("• Conectando a Oracle")
		case "inspecting":
			appendLog("• Inspeccionando esquema")
		case "exporting":
			appendLog("• Exportando Parquet")
		case "finalizing":
			appendLog("• Finalizando exportación")
		}
	}

	renderResult := func(res exportResult) {
		lastResult = &res
		fyne.Do(func() {
			statusLabel.SetText(fmt.Sprintf("Listo. %d filas en %s", res.Rows, formatBytes(res.FileSizeBytes)))
			progress.SetValue(1)
			progressLabel.SetText("Exportación completada")
			setRunning(false)
			setFieldState(true)
		})
		appendLog("• Resultado: %s", res.Output)
		appendLog("• Filas: %d", res.Rows)
		appendLog("• Tamaño: %s", formatBytes(res.FileSizeBytes))
		appendLog("• Tiempo total: %s", res.TotalTime.Truncate(time.Millisecond))
		appendLog("• Velocidad: %.2f filas/seg", res.Speed)
	}

	renderError := func(err error) {
		fyne.Do(func() {
			statusLabel.SetText("La exportación falló")
			progressLabel.SetText(err.Error())
			setRunning(false)
			setFieldState(true)
		})
		appendLog("ERROR: %v", err)
	}

	openSQL := func() {
		dialog.ShowFileOpen(func(rc fyne.URIReadCloser, err error) {
			if err != nil || rc == nil {
				return
			}
			defer rc.Close()
			data, readErr := os.ReadFile(rc.URI().Path())
			if readErr != nil {
				dialog.ShowError(readErr, w)
				return
			}
			fyne.Do(func() {
				sqlPath.SetText(rc.URI().Path())
				queryEntry.SetText(string(data))
				statusLabel.SetText("SQL cargado desde disco")
			})
			appendLog("• SQL cargado: %s", rc.URI().Path())
		}, w)
	}

	saveOutput := func() {
		dialog.ShowFileSave(func(wc fyne.URIWriteCloser, err error) {
			if err != nil || wc == nil {
				return
			}
			defer wc.Close()
			path := wc.URI().Path()
			if !strings.HasSuffix(strings.ToLower(path), ".parquet") {
				path += ".parquet"
			}
			fyne.Do(func() {
				outPath.SetText(path)
				statusLabel.SetText("Destino configurado")
			})
			appendLog("• Salida: %s", path)
		}, w)
	}

	openFolder := func() {
		if strings.TrimSpace(outPath.Text) == "" {
			dialog.ShowInformation("Abrir carpeta", "Primero elige una salida .parquet.", w)
			return
		}
		_ = openPath(filepath.Dir(outPath.Text))
	}

	exportBtn.OnTapped = func() {
		if running {
			return
		}

		formUser := strings.TrimSpace(userEntry.Text)
		formPass := passEntry.Text
		formConn := strings.TrimSpace(connEntry.Text)
		formHost := strings.TrimSpace(hostEntry.Text)
		formService := strings.TrimSpace(serviceEntry.Text)
		formQuery := strings.TrimSpace(queryEntry.Text)
		formOut := strings.TrimSpace(outPath.Text)

		formPort, err := strconv.Atoi(strings.TrimSpace(portEntry.Text))
		if err != nil {
			dialog.ShowError(fmt.Errorf("puerto inválido"), w)
			return
		}
		formTimeout, err := time.ParseDuration(strings.TrimSpace(timeoutEntry.Text))
		if err != nil {
			dialog.ShowError(fmt.Errorf("tiempo límite inválido"), w)
			return
		}
		if formUser == "" || formPass == "" {
			dialog.ShowError(fmt.Errorf("faltan credenciales de Oracle"), w)
			return
		}
		if formQuery == "" {
			dialog.ShowError(fmt.Errorf("la consulta SQL no puede estar vacía"), w)
			return
		}
		if formOut == "" {
			dialog.ShowError(fmt.Errorf("elige una ruta de salida"), w)
			return
		}
		if !strings.HasSuffix(strings.ToLower(formOut), ".parquet") {
			formOut += ".parquet"
			outPath.SetText(formOut)
		}

		setRunning(true)
		setFieldState(false)
		progress.SetValue(0)
		progressLabel.SetText("En cola")
		statusLabel.SetText("Exportación en curso")
		appendLog("")
		appendLog("Iniciando exportación...")

		go func() {
			res, err := executeExport(
				context.Background(),
				exportDefaults{
					User:    formUser,
					Pass:    formPass,
					ConnStr: formConn,
					Host:    formHost,
					Port:    formPort,
					Service: formService,
					Query:   formQuery,
					Out:     formOut,
					Timeout: formTimeout,
				},
				updateProgress,
			)
			if err != nil {
				renderError(err)
				return
			}
			renderResult(res)
		}()
	}

	openSqlBtn.OnTapped = openSQL
	saveBtn.OnTapped = saveOutput
	openOutBtn.OnTapped = openFolder

	if strings.TrimSpace(outPath.Text) == "" {
		outPath.SetText(defaults.Out)
	}
	if strings.TrimSpace(queryEntry.Text) == "" {
		queryEntry.SetText(defaults.Query)
	}
	setFieldState(true)

	content := container.NewVBox(
		widget.NewLabelWithStyle("Exportador Oracle -> Parquet", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		widget.NewLabel("Selecciona un SQL local, define el archivo de salida y ejecuta la exportación desde tu escritorio."),
		widget.NewSeparator(),
		container.NewGridWithColumns(2, openSqlBtn, saveBtn),
		sqlPath,
		outPath,
		widget.NewSeparator(),
		container.NewGridWithColumns(2, userEntry, passEntry),
		container.NewGridWithColumns(2, hostEntry, serviceEntry),
		container.NewGridWithColumns(2, portEntry, timeoutEntry),
		connEntry,
		widget.NewSeparator(),
		queryEntry,
		container.NewHBox(exportBtn, openOutBtn, clearBtn, layout.NewSpacer()),
		progress,
		progressLabel,
		statusLabel,
		widget.NewSeparator(),
		widget.NewLabelWithStyle("Registro", fyne.TextAlignLeading, fyne.TextStyle{Bold: true}),
		logBox,
	)

	w.SetContent(container.NewPadded(container.NewVScroll(content)))
	w.ShowAndRun()
	_ = lastResult
	return nil
}

func openPath(path string) error {
	switch runtime.GOOS {
	case "windows":
		return exec.Command("explorer", path).Start()
	case "darwin":
		return exec.Command("open", path).Start()
	default:
		return exec.Command("xdg-open", path).Start()
	}
}
