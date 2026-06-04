package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"net/http"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// ─── Tipos internos ───────────────────────────────────────────────────────────

type schemaNode struct {
	Tag    string       `json:"Tag"`
	Fields []schemaNode `json:"Fields,omitempty"`
}

type columnKind int

const (
	columnKindString         columnKind = iota
	columnKindTimestampMicros
	columnKindBool
	columnKindInt64
	columnKindFloat64
	columnKindBinary // BLOB / RAW → base64 string
)

type columnSpec struct {
	Name string
	Kind columnKind
}

const softwareAuthor = "Christian Reinaldo Ruiz Buitron"

// ─── Entry point ──────────────────────────────────────────────────────────────

func main() {
	if err := run(); err != nil {
		fatalError(err)
	}
	waitForEnter()
}

func run() error {
	loadDotEnv(".env")

	var (
		web     = flag.Bool("web", false, "levanta la interfaz web en vez del flujo de escritorio")
		user    = flag.String("user", os.Getenv("ORACLE_USER"), "usuario Oracle")
		pass    = flag.String("pass", os.Getenv("ORACLE_PASSWORD"), "clave Oracle")
		connStr = flag.String("connstr", envOrString("ORACLE_CONN_STR", ""), "descriptor JDBC/Oracle completo")
		host    = flag.String("host", envOrString("ORACLE_HOST", "192.168.100.219"), "host Oracle")
		port    = flag.Int("port", envOrInt("ORACLE_PORT", 1521), "puerto Oracle")
		service = flag.String("service", envOrString("ORACLE_SERVICE", "orcl"), "service name Oracle")
		query   = flag.String("query", envOrString("ORACLE_QUERY", "SELECT * FROM TCACHE"), "consulta SQL")
		out     = flag.String("out", envOrString("ORACLE_OUT", "query.parquet"), "archivo Parquet de salida")
		timeout = flag.Duration("timeout", 5*time.Minute, "timeout total de la consulta")
	)
	flag.Parse()

	if *web {
		return serveWebApp(*user, *pass, *connStr, *host, *port, *service, *query, *out, *timeout)
	}

	return runDesktopApp(*user, *pass, *connStr, *host, *port, *service, *query, *out, *timeout)
}

// ─── Configuración y entorno ──────────────────────────────────────────────────

func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		log.Printf("no se pudo leer %s: %v", path, err)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}
		value = strings.Trim(value, `"'`)
		if current, exists := os.LookupEnv(key); !exists || strings.TrimSpace(current) == "" {
			if err := os.Setenv(key, value); err != nil {
				log.Printf("no se pudo establecer %s desde %s: %v", key, path, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Printf("error leyendo %s: %v", path, err)
	}
}

func envOrString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// ─── Conexión Oracle ──────────────────────────────────────────────────────────

func openOracle(connStr, host string, port int, service, user, pass string) (*sql.DB, error) {
	var connURL string
	if strings.TrimSpace(connStr) != "" {
		connURL = go_ora.BuildJDBC(user, pass, connStr, nil)
	} else {
		connURL = go_ora.BuildUrl(host, port, service, user, pass, nil)
	}
	return sql.Open("oracle", connURL)
}

// ─── Inferencia de tipos ──────────────────────────────────────────────────────

func inferColumnSpecsTyped(rows *sql.Rows, columns []string) []columnSpec {
	specs := make([]columnSpec, len(columns))
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Printf("no se pudieron leer los tipos de columnas; se exportarán como texto: %v", err)
		columnTypes = nil
	}
	for i, col := range columns {
		spec := columnSpec{
			Name: sanitizeName(col),
			Kind: columnKindString,
		}
		if i < len(columnTypes) {
			spec.Kind = inferColumnKindTyped(columnTypes[i], col)
		}
		specs[i] = spec
	}
	return specs
}

func inferColumnKindTyped(ct *sql.ColumnType, columnName string) columnKind {
	if ct == nil {
		return columnKindString
	}

	dbType := strings.ToUpper(strings.TrimSpace(ct.DatabaseTypeName()))
	length, lengthOK := ct.Length()
	precision, scale, decimalOK := ct.DecimalSize()
	lowerName := strings.ToLower(strings.TrimSpace(columnName))

	// ── Tipos binarios (BLOB, RAW) → base64 string en Parquet ────────────────
	switch dbType {
	case "BLOB", "RAW", "LONG RAW":
		return columnKindBinary

	// ── LOB de texto y tipos legacy de texto largo ────────────────────────────
	case "CLOB", "NCLOB", "LONG":
		return columnKindString

	// ── Booleano nativo (no existe en Oracle, pero el driver puede reportarlo) ─
	case "BOOLEAN", "BOOL", "BIT":
		return columnKindBool

	// ── Tipos temporales ──────────────────────────────────────────────────────
	case "DATE":
		return columnKindTimestampMicros
	}

	if strings.Contains(dbType, "TIMESTAMP") || strings.Contains(dbType, "INTERVAL") {
		// INTERVAL no tiene representación numérica estándar en Parquet;
		// lo guardamos como cadena legible (el driver lo devuelve como string).
		if strings.Contains(dbType, "INTERVAL") {
			return columnKindString
		}
		return columnKindTimestampMicros
	}

	// ── Flotantes nativos de Oracle ───────────────────────────────────────────
	if strings.Contains(dbType, "BINARY_FLOAT") || strings.Contains(dbType, "BINARY_DOUBLE") ||
		strings.Contains(dbType, "FLOAT") || strings.Contains(dbType, "DOUBLE") ||
		strings.Contains(dbType, "REAL") {
		return columnKindFloat64
	}

	// ── NUMBER / DECIMAL / NUMERIC ────────────────────────────────────────────
	if strings.Contains(dbType, "NUMBER") || strings.Contains(dbType, "DECIMAL") ||
		strings.Contains(dbType, "NUMERIC") {

		// NUMBER(1,0) con nombre sugestivo → booleano
		if decimalOK && scale == 0 && precision == 1 {
			return columnKindBool
		}
		// Tiene decimales declarados → flotante
		if decimalOK && scale > 0 {
			return columnKindFloat64
		}
		// Entero declarado (scale == 0, precision > 1)
		if decimalOK && scale == 0 && precision > 1 {
			return columnKindInt64
		}
		// FIX: NUMBER sin metadatos de precisión/escala (declarado como NUMBER a secas).
		// Oracle 11g frecuentemente omite estos metadatos. Usamos float64 para no
		// truncar silenciosamente valores decimales como 123.45.
		return columnKindFloat64
	}

	// ── CHAR/VARCHAR de longitud 1 con nombre sugestivo → booleano ───────────
	switch dbType {
	case "CHAR", "NCHAR", "VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2":
		if lengthOK && length == 1 && isBooleanLikeColumnName(lowerName) {
			return columnKindBool
		}
		return columnKindString
	}

	// ── Fallback por ScanType del driver ──────────────────────────────────────
	if scanType := ct.ScanType(); scanType != nil {
		if scanType.Kind() == reflect.Ptr {
			scanType = scanType.Elem()
		}
		switch scanType {
		case reflect.TypeOf(time.Time{}), reflect.TypeOf(sql.NullTime{}):
			return columnKindTimestampMicros
		case reflect.TypeOf(true), reflect.TypeOf(sql.NullBool{}):
			return columnKindBool
		case reflect.TypeOf(float32(0)), reflect.TypeOf(float64(0)), reflect.TypeOf(sql.NullFloat64{}):
			return columnKindFloat64
		case reflect.TypeOf(int(0)), reflect.TypeOf(int8(0)), reflect.TypeOf(int16(0)),
			reflect.TypeOf(int32(0)), reflect.TypeOf(int64(0)),
			reflect.TypeOf(uint(0)), reflect.TypeOf(uint8(0)), reflect.TypeOf(uint16(0)),
			reflect.TypeOf(uint32(0)), reflect.TypeOf(uint64(0)), reflect.TypeOf(sql.NullInt64{}):
			return columnKindInt64
		}
		switch scanType.Kind() {
		case reflect.Bool:
			return columnKindBool
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return columnKindInt64
		case reflect.Float32, reflect.Float64:
			return columnKindFloat64
		}
	}

	return columnKindString
}

func isBooleanLikeColumnName(name string) bool {
	patterns := []string{"flag", "is_", "has_", "can_", "enable", "active", "activo", "ind_", "bool", "verdad"}
	for _, p := range patterns {
		if strings.Contains(name, p) {
			return true
		}
	}
	return false
}

// ─── Construcción del esquema Parquet ─────────────────────────────────────────

func buildSchemaTyped(columns []columnSpec) (string, []string, error) {
	fields := make([]schemaNode, 0, len(columns))
	names := make([]string, 0, len(columns))
	used := make(map[string]int, len(columns))

	for _, col := range columns {
		name := col.Name
		if name == "" {
			name = "col"
		}
		base := name
		if n := used[base]; n > 0 {
			name = base + "_" + strconv.Itoa(n+1)
		}
		used[base]++
		names = append(names, name)

		var tag string
		switch col.Kind {
		case columnKindTimestampMicros:
			tag = fmt.Sprintf("name=%s, type=INT64, convertedtype=TIMESTAMP_MICROS, repetitiontype=OPTIONAL", name)
		case columnKindBool:
			tag = fmt.Sprintf("name=%s, type=BOOLEAN, repetitiontype=OPTIONAL", name)
		case columnKindInt64:
			tag = fmt.Sprintf("name=%s, type=INT64, repetitiontype=OPTIONAL", name)
		case columnKindFloat64:
			tag = fmt.Sprintf("name=%s, type=DOUBLE, repetitiontype=OPTIONAL", name)
		default: // columnKindString, columnKindBinary (base64 → UTF8)
			tag = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL", name)
		}
		fields = append(fields, schemaNode{Tag: tag})
	}

	schema := schemaNode{Tag: "name=query_result", Fields: fields}
	raw, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return "", nil, fmt.Errorf("no se pudo construir el esquema parquet: %w", err)
	}
	return string(raw), names, nil
}

// ─── Conversión de valores ────────────────────────────────────────────────────

func valueToJSONTyped(v any, kind columnKind) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch kind {
	case columnKindTimestampMicros:
		return timestampValueToMicros(v)
	case columnKindBool:
		return boolValueTyped(v)
	case columnKindInt64:
		return int64ValueTyped(v)
	case columnKindFloat64:
		return float64ValueTyped(v)
	case columnKindBinary:
		return binaryValueTyped(v)
	}
	// columnKindString
	switch t := v.(type) {
	case []byte:
		return string(t), nil
	case string:
		return t, nil
	case fmt.Stringer:
		return t.String(), nil
	default:
		return fmt.Sprint(t), nil
	}
}

// binaryValueTyped convierte BLOB/RAW a base64 para almacenarlo como string UTF-8 en Parquet.
func binaryValueTyped(v any) (any, error) {
	switch t := v.(type) {
	case []byte:
		return base64.StdEncoding.EncodeToString(t), nil
	case string:
		// go-ora a veces devuelve RAW pequeño como string; lo re-encodemos.
		return base64.StdEncoding.EncodeToString([]byte(t)), nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("tipo binario no soportado: %T", v)
	}
}

func timestampValueToMicros(v any) (any, error) {
	switch t := v.(type) {
	case time.Time:
		return t.UTC().UnixNano() / int64(time.Microsecond), nil
	case sql.NullTime:
		if !t.Valid {
			return nil, nil
		}
		return t.Time.UTC().UnixNano() / int64(time.Microsecond), nil
	case []byte:
		return parseTimestampString(string(t))
	case string:
		return parseTimestampString(t)
	case fmt.Stringer:
		return parseTimestampString(t.String())
	default:
		return nil, fmt.Errorf("tipo temporal no soportado: %T", v)
	}
}

func parseTimestampString(value string) (any, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999 -07:00",
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC().UnixNano() / int64(time.Microsecond), nil
		}
	}
	return nil, fmt.Errorf("no se pudo interpretar la fecha %q", value)
}

func boolValueTyped(v any) (any, error) {
	switch t := v.(type) {
	case bool:
		return t, nil
	case sql.NullBool:
		if !t.Valid {
			return nil, nil
		}
		return t.Bool, nil
	case []byte:
		return parseBoolString(string(t))
	case string:
		return parseBoolString(t)
	case fmt.Stringer:
		return parseBoolString(t.String())
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, nil
	}
	switch rv.Kind() {
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int() != 0, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint() != 0, nil
	case reflect.Float32, reflect.Float64:
		return rv.Float() != 0, nil
	}
	return nil, fmt.Errorf("tipo booleano no soportado: %T", v)
}

func int64ValueTyped(v any) (any, error) {
	switch t := v.(type) {
	case int:
		return int64(t), nil
	case int8:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case int64:
		return t, nil
	case uint:
		return int64(t), nil
	case uint8:
		return int64(t), nil
	case uint16:
		return int64(t), nil
	case uint32:
		return int64(t), nil
	case uint64:
		return int64(t), nil
	case sql.NullInt64:
		if !t.Valid {
			return nil, nil
		}
		return t.Int64, nil
	case []byte:
		return parseInt64String(string(t))
	case string:
		return parseInt64String(t)
	case fmt.Stringer:
		return parseInt64String(t.String())
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, nil
	}
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(rv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return int64(rv.Float()), nil
	}
	return nil, fmt.Errorf("tipo entero no soportado: %T", v)
}

func float64ValueTyped(v any) (any, error) {
	switch t := v.(type) {
	case float32:
		return float64(t), nil
	case float64:
		return t, nil
	case sql.NullFloat64:
		if !t.Valid {
			return nil, nil
		}
		return t.Float64, nil
	case []byte:
		return parseFloat64String(string(t))
	case string:
		return parseFloat64String(t)
	case fmt.Stringer:
		return parseFloat64String(t.String())
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, nil
	}
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), nil
	}
	return nil, fmt.Errorf("tipo decimal no soportado: %T", v)
}

// ─── Parseo de cadenas ────────────────────────────────────────────────────────

func parseBoolString(value string) (any, error) {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return nil, nil
	}
	switch value {
	case "1", "t", "true", "y", "yes", "si", "s":
		return true, nil
	case "0", "f", "false", "n", "no":
		return false, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return nil, fmt.Errorf("no se pudo interpretar el booleano %q", value)
	}
	return parsed, nil
}

func parseInt64String(value string) (any, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsed, nil
	}
	// FIX: go-ora puede devolver NUMBER entero como "123.0"; lo casteamos sin perder datos.
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return int64(f), nil
	}
	return nil, fmt.Errorf("no se pudo interpretar el entero %q", value)
}

func parseFloat64String(value string) (any, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("no se pudo interpretar el decimal %q", value)
	}
	return parsed, nil
}

// ─── Utilidades de nombre ─────────────────────────────────────────────────────

var nonIdent = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func sanitizeName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, ".", "_")
	name = strings.ReplaceAll(name, "$", "_")
	name = nonIdent.ReplaceAllString(name, "_")
	name = strings.Trim(name, "_")
	if name == "" {
		return ""
	}
	if name[0] >= '0' && name[0] <= '9' {
		name = "c_" + name
	}
	return strings.ToLower(name)
}

// ─── Vista previa del Parquet generado ───────────────────────────────────────

func previewParquetRows(filePath string, numRows int) error {
	fileReader, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return fmt.Errorf("error al abrir el parquet para vista previa: %w", err)
	}
	defer fileReader.Close()

	pr, err := reader.NewParquetReader(fileReader, nil, 1)
	if err != nil {
		return fmt.Errorf("error al inicializar el lector parquet: %w", err)
	}
	defer pr.ReadStop()

	totalRows := int(pr.GetNumRows())
	if totalRows == 0 {
		fmt.Println("   [El archivo Parquet está vacío]")
		return nil
	}
	if numRows > totalRows {
		numRows = totalRows
	}
	res, err := pr.ReadByNumber(numRows)
	if err != nil {
		return fmt.Errorf("error leyendo las primeras filas del parquet: %w", err)
	}
	jsonBs, err := json.MarshalIndent(res, "   ", "  ")
	if err != nil {
		return fmt.Errorf("error formateando la vista previa del parquet: %w", err)
	}
	fmt.Println(string(jsonBs))
	return nil
}

// ─── Presentación y utilidades de terminal ────────────────────────────────────

func fatalError(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "\nERROR CRÍTICO: %v\n", err)
	waitForEnter()
	os.Exit(1)
}

func waitForEnter() {
	if !stdinIsInteractive() {
		return
	}
	fmt.Print("\nPresiona Enter para salir...")
	r := bufio.NewReader(os.Stdin)
	_, _ = r.ReadString('\n')
}

func stdinIsInteractive() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return runtime.GOOS == "windows"
	}
	return info.Mode()&os.ModeCharDevice != 0
}

func rowsPerSecond(count int64, start time.Time) float64 {
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed
}

func queryElapsed(start time.Time) time.Duration {
	return time.Since(start).Truncate(time.Millisecond)
}

func formatBytes(size int64) string {
	if size < 0 {
		return "0 B"
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	value := float64(size)
	unit := 0
	for value >= 1024 && unit < len(units)-1 {
		value /= 1024
		unit++
	}
	if unit == 0 {
		return fmt.Sprintf("%d %s", size, units[unit])
	}
	return fmt.Sprintf("%.2f %s", value, units[unit])
}

type exportDefaults struct {
	User    string
	Pass    string
	ConnStr string
	Host    string
	Port    int
	Service string
	Query   string
	Out     string
	Timeout time.Duration
}

type appViewModel struct {
	Title             string
	Defaults          exportDefaults
	HasPreview        bool
	Preview           exportPlan
	LastResult        *exportResult
	LastError         string
	SuccessMessage    string
	CurrentPath       string
	DownloadAvailable bool
	Job               *exportJobView
}

type exportResult struct {
	Output        string
	FileSizeBytes  int64
	Rows          int64
	QueryTime     time.Duration
	WriteTime     time.Duration
	TotalTime     time.Duration
	Speed         float64
	Columns       int
	TypeMix       string
	PreviewRows   string
	ColumnSummary string
}

type exportJobStatus string

const (
	jobIdle      exportJobStatus = "idle"
	jobQueued    exportJobStatus = "queued"
	jobRunning   exportJobStatus = "running"
	jobFailed    exportJobStatus = "failed"
	jobCompleted exportJobStatus = "completed"
)

type exportJobPhase struct {
	Key        string `json:"key"`
	Label      string `json:"label"`
	Percent    int    `json:"percent"`
	Active     bool   `json:"active"`
	Completed  bool   `json:"completed"`
}

type exportJobView struct {
	ID          string            `json:"id"`
	Status      exportJobStatus   `json:"status"`
	Message     string            `json:"message"`
	Progress    int               `json:"progress"`
	Phases      []exportJobPhase  `json:"phases"`
	DownloadURL string            `json:"download_url,omitempty"`
	Result      *exportResult     `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
	StartedAt   time.Time         `json:"started_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	FinishedAt  time.Time         `json:"finished_at,omitempty"`
}

type jobState struct {
	mu      sync.Mutex
	defaults exportDefaults
	job     *exportJobView
}

func serveWebApp(user, pass, connStr, host string, port int, service, query, out string, timeout time.Duration) error {
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

	tmpl, err := template.New("app").Funcs(template.FuncMap{
		"humanBytes": formatBytes,
		"humanDur":   func(d time.Duration) string { return d.Truncate(time.Millisecond).String() },
		"pct":        func(n int) string { return fmt.Sprintf("%d%%", n) },
	}).Parse(appTemplate)
	if err != nil {
		return fmt.Errorf("no se pudo preparar la interfaz web: %w", err)
	}

	mux := http.NewServeMux()
	state := &jobState{defaults: defaults}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		defer state.mu.Unlock()

		vm := appViewModel{
			Title:             "Dejemevivir Exporter",
			Defaults:          state.defaults,
			HasPreview:        state.job != nil && state.job.Result != nil,
			LastResult:        jobResult(state.job),
			LastError:         jobError(state.job),
			CurrentPath:       r.URL.Path,
			DownloadAvailable: state.job != nil && state.job.Result != nil,
			Job:               state.job,
		}
		if err := tmpl.Execute(w, vm); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/export", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}

		form := exportDefaults{
			User:    r.FormValue("user"),
			Pass:    r.FormValue("pass"),
			ConnStr: r.FormValue("connstr"),
			Host:    r.FormValue("host"),
			Service: r.FormValue("service"),
			Query:   r.FormValue("query"),
			Out:     r.FormValue("out"),
		}
		if p, err := strconv.Atoi(r.FormValue("port")); err == nil {
			form.Port = p
		} else {
			form.Port = defaults.Port
		}
		if d, err := time.ParseDuration(r.FormValue("timeout")); err == nil {
			form.Timeout = d
		} else {
			form.Timeout = defaults.Timeout
		}
		if strings.TrimSpace(form.User) == "" {
			form.User = defaults.User
		}
		if strings.TrimSpace(form.Pass) == "" {
			form.Pass = defaults.Pass
		}
		if strings.TrimSpace(form.ConnStr) == "" {
			form.ConnStr = defaults.ConnStr
		}
		if strings.TrimSpace(form.Host) == "" {
			form.Host = defaults.Host
		}
		if strings.TrimSpace(form.Service) == "" {
			form.Service = defaults.Service
		}
		if strings.TrimSpace(form.Query) == "" {
			form.Query = defaults.Query
		}
		if strings.TrimSpace(form.Out) == "" {
			form.Out = defaults.Out
		}

		state.mu.Lock()
		job := newExportJob(form)
		state.defaults = form
		state.job = job
		state.mu.Unlock()

		go func() {
			res, err := executeExport(context.Background(), form, func(phase string, percent int, message string) {
				state.mu.Lock()
				defer state.mu.Unlock()
				if state.job != nil && state.job.ID == job.ID {
					updateJobProgress(state.job, phase, percent, message)
				}
			})

			state.mu.Lock()
			defer state.mu.Unlock()
			if state.job == nil || state.job.ID != job.ID {
				return
			}
			if err != nil {
				failJob(state.job, err)
			} else {
				completeJob(state.job, res)
			}
		}()

		http.Redirect(w, r, "/?started=1", http.StatusSeeOther)
	})

	mux.HandleFunc("/download", func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		result := jobResult(state.job)
		state.mu.Unlock()

		if result == nil {
			http.Error(w, "Todavía no hay un archivo disponible para descargar.", http.StatusNotFound)
			return
		}

		filePath := strings.TrimSpace(result.Output)
		if filePath == "" {
			http.Error(w, "La exportación no tiene una ruta de salida válida.", http.StatusInternalServerError)
			return
		}

		file, err := os.Open(filePath)
		if err != nil {
			http.Error(w, fmt.Sprintf("No se pudo abrir el archivo: %v", err), http.StatusInternalServerError)
			return
		}
		defer file.Close()

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, filepath.Base(filePath)))
		http.ServeFile(w, r, filePath)
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		state.mu.Lock()
		job := state.job
		state.mu.Unlock()
		if job == nil {
			job = newExportJob(defaults)
			job.Status = jobIdle
			job.Message = "Listo para exportar"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(job)
	})

	addr := envOrString("WEB_ADDR", "127.0.0.1:3000")
	fmt.Printf("UI web lista en http://%s\n", addr)
	return http.ListenAndServe(addr, mux)
}

func executeExport(ctx context.Context, defaults exportDefaults, progress func(phase string, percent int, message string)) (exportResult, error) {
	ctx, cancel := context.WithTimeout(ctx, defaults.Timeout)
	defer cancel()

	startTime := time.Now()
	if progress != nil {
		progress("connecting", 12, "Conectando a Oracle")
	}
	db, err := openOracle(defaults.ConnStr, defaults.Host, defaults.Port, defaults.Service, defaults.User, defaults.Pass)
	if err != nil {
		return exportResult{}, fmt.Errorf("error conectando a Oracle: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return exportResult{}, fmt.Errorf("no se pudo hacer ping a Oracle: %w", err)
	}

	if progress != nil {
		progress("inspecting", 28, "Inspeccionando esquema")
	}
	queryStart := time.Now()
	rows, err := db.QueryContext(ctx, defaults.Query)
	if err != nil {
		return exportResult{}, fmt.Errorf("error ejecutando query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return exportResult{}, fmt.Errorf("no se pudieron leer columnas: %w", err)
	}
	columnSpecs := inferColumnSpecsTyped(rows, columns)
	schemaJSON, parquetNames, err := buildSchemaTyped(columnSpecs)
	if err != nil {
		return exportResult{}, err
	}

	if progress != nil {
		progress("exporting", 42, "Escribiendo Parquet")
	}
	f, err := os.Create(defaults.Out)
	if err != nil {
		return exportResult{}, fmt.Errorf("no se pudo crear archivo de salida: %w", err)
	}
	pw, err := writer.NewJSONWriter(schemaJSON, writerfile.NewWriterFile(f), 4)
	if err != nil {
		_ = f.Close()
		return exportResult{}, fmt.Errorf("no se pudo crear writer parquet: %w", err)
	}
	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED

	values := make([]any, len(columns))
	scanArgs := make([]any, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	writeStart := time.Now()
	var count int64
	for rows.Next() {
		for i := range values {
			values[i] = nil
		}
		if err := rows.Scan(scanArgs...); err != nil {
			_ = pw.WriteStop()
			_ = f.Close()
			return exportResult{}, fmt.Errorf("error leyendo fila: %w", err)
		}
		payload := make(map[string]any, len(columns))
		for i, col := range parquetNames {
			converted, err := valueToJSONTyped(values[i], columnSpecs[i].Kind)
			if err != nil {
				_ = pw.WriteStop()
				_ = f.Close()
				return exportResult{}, fmt.Errorf("error convirtiendo columna %s: %w", col, err)
			}
			payload[col] = converted
		}
		rec, err := json.Marshal(payload)
		if err != nil {
			_ = pw.WriteStop()
			_ = f.Close()
			return exportResult{}, fmt.Errorf("error serializando fila a JSON: %w", err)
		}
		if err := pw.Write(string(rec)); err != nil {
			_ = pw.WriteStop()
			_ = f.Close()
			return exportResult{}, fmt.Errorf("error escribiendo parquet: %w", err)
		}
		count++
		if progress != nil && count%1000 == 0 {
			progress("exporting", 42, fmt.Sprintf("Procesadas %d filas", count))
		}
	}
	if err := rows.Err(); err != nil {
		_ = pw.WriteStop()
		_ = f.Close()
		return exportResult{}, fmt.Errorf("error iterando filas: %w", err)
	}
	if err := pw.WriteStop(); err != nil {
		_ = f.Close()
		return exportResult{}, fmt.Errorf("error cerrando parquet: %w", err)
	}
	if err := f.Close(); err != nil {
		return exportResult{}, fmt.Errorf("no se pudo cerrar el archivo de salida: %w", err)
	}
	if progress != nil {
		progress("finalizing", 90, "Validando y cerrando salida")
	}
	if progress != nil {
		progress("finalizing", 100, "Exportación completada")
	}

	fileInfo, _ := os.Stat(defaults.Out)
	size := int64(0)
	if fileInfo != nil {
		size = fileInfo.Size()
	}
	return exportResult{
		Output:       defaults.Out,
		FileSizeBytes: size,
		Rows:         count,
		QueryTime:    queryElapsed(queryStart),
		WriteTime:    time.Since(writeStart),
		TotalTime:    time.Since(startTime),
		Speed:        rowsPerSecond(count, writeStart),
		Columns:      len(columns),
		TypeMix:      summarizeTypeMix(columnSpecs),
		PreviewRows:  "",
		ColumnSummary: fmt.Sprintf("%d columns", len(columns)),
	}, nil
}

const appTemplate = `
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{.Title}}</title>
  <style>
    :root{--bg:#f6f3ed;--panel:#ffffff;--panel2:#fbfaf7;--text:#111827;--muted:#5f6b7a;--line:rgba(17,24,39,.08);--accent:#0f766e;--accent2:#2563eb;--warn:#b45309;--danger:#b91c1c}
    *{box-sizing:border-box} body{margin:0;font-family:Inter,ui-sans-serif,system-ui,sans-serif;background:linear-gradient(180deg,#f8f5ef 0,#f4efe7 100%);color:var(--text)}
    .wrap{max-width:1120px;margin:0 auto;padding:42px 24px 56px}
    .hero{display:grid;grid-template-columns:1.2fr .8fr;gap:20px;align-items:stretch}
    .section{padding:30px 0;border-top:1px solid rgba(17,24,39,.06)}
    .section:first-child{border-top:0;padding-top:0}
    .hero-main{padding:0}
    .eyebrow{display:inline-flex;gap:8px;align-items:center;padding:8px 12px;border-radius:999px;background:rgba(15,118,110,.08);color:var(--accent);font-size:12px;font-weight:700;letter-spacing:.12em;text-transform:uppercase}
    h1{margin:14px 0 10px;font-size:clamp(2.4rem,5vw,4.8rem);line-height:.96;letter-spacing:-.05em}
    .sub{max-width:64ch;color:var(--muted);font-size:1.06rem;line-height:1.6}
    .stats{display:grid;grid-template-columns:repeat(4,1fr);gap:18px;margin-top:22px}
    .stat{padding:12px 0;border-top:1px solid rgba(17,24,39,.08)}
    .stat .k{font-size:.76rem;color:var(--muted);text-transform:uppercase;letter-spacing:.12em}
    .stat .v{margin-top:8px;font-size:1.2rem;font-weight:700}
    .stack{display:grid;gap:20px}
    .panel{padding:0}
    .grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
    label{display:block;font-size:.8rem;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:8px}
    input,textarea{width:100%;background:var(--panel);border:1px solid var(--line);border-radius:14px;color:var(--text);padding:14px 16px;font:inherit;outline:none}
    textarea{min-height:150px;resize:vertical}
    input:focus,textarea:focus{border-color:rgba(124,242,195,.5);box-shadow:0 0 0 4px rgba(124,242,195,.08)}
    .row{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
    .btn{appearance:none;border:0;border-radius:16px;padding:14px 18px;font:700 0.95rem/1 Inter,system-ui,sans-serif;cursor:pointer}
    .btn.primary{background:var(--text);color:#fff;box-shadow:none}
    .btn.secondary{background:transparent;color:var(--text);border:1px solid var(--line)}
    .btn.link{display:inline-flex;align-items:center;justify-content:center;text-decoration:none}
    .btn[disabled], .btn.disabled{opacity:.55;cursor:not-allowed;pointer-events:none}
    .list{display:grid;gap:8px;margin-top:8px}
    .pill{display:block;padding:0;color:var(--muted);font-size:.92rem}
    .section-title{display:flex;justify-content:space-between;align-items:end;gap:12px;margin-bottom:10px}
    .section-title h2{margin:0;font-size:1.1rem;letter-spacing:-.02em}
    .section-title span{color:var(--muted);font-size:.9rem}
    .table{width:100%;border-collapse:collapse}
    .table td,.table th{border-bottom:1px solid rgba(17,24,39,.06);padding:10px 0;text-align:left;font-size:.92rem}
    .table th{color:var(--muted);font-size:.75rem;text-transform:uppercase;letter-spacing:.12em}
    .notice{padding:0;color:#0f3f3b}
    .error{padding:0;color:#7f1d1d}
    .progress-wrap{margin:10px 0 6px}
    .progress-track{height:10px;border-radius:999px;background:rgba(17,24,39,.06);overflow:hidden}
    .progress-fill{height:100%;width:0%;border-radius:999px;background:var(--text);transition:width .35s ease}
    .progress-meta{display:flex;justify-content:space-between;gap:12px;align-items:center;margin-top:8px;color:var(--muted);font-size:.9rem}
    .phase-list{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-top:12px}
    .phase{padding:8px 0;border-top:1px solid rgba(17,24,39,.06);font-size:.82rem;color:var(--muted);text-align:left}
    .phase.active{color:var(--text);border-top-color:rgba(17,24,39,.22)}
    .phase.done{color:#1f2937}
    @media (max-width: 980px){.hero{grid-template-columns:1fr}.stats{grid-template-columns:1fr 1fr}.grid2{grid-template-columns:1fr}}
    @media (max-width: 640px){.stats{grid-template-columns:1fr}.wrap{padding:20px 16px 32px}h1{font-size:2.3rem}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="section">
      <div class="hero-main">
        <div class="eyebrow">Espacio premium de exportación Oracle</div>
        <h1>Exporta datos con confianza, no con ritual.</h1>
        <p class="sub">Conecta Oracle, previsualiza el esquema, ejecuta la exportación y valida el resultado desde un solo espacio premium. El objetivo no es mover archivos. El objetivo es reducir riesgo, tiempo e incertidumbre.</p>
        <div class="stats">
          <div class="stat"><div class="k">Última exportación</div><div class="v">{{if .LastResult}}{{.LastResult.Rows}} filas{{else}}Aún no hay ejecuciones{{end}}</div></div>
          <div class="stat"><div class="k">Salida</div><div class="v">{{.Defaults.Out}}</div></div>
          <div class="stat"><div class="k">Conexión</div><div class="v">{{if .Defaults.ConnStr}}Descriptor{{else}}{{.Defaults.Host}}:{{.Defaults.Port}}{{end}}</div></div>
          <div class="stat"><div class="k">Tiempo límite</div><div class="v">{{humanDur .Defaults.Timeout}}</div></div>
        </div>
      </div>
    </div>

    <div class="section">
        <div class="section-title"><h2>Acciones rápidas</h2><span>Diseñado para sentirse inmediato</span></div>
        <div class="progress-wrap">
        <div class="progress-track"><div id="progressFill" class="progress-fill" style="width:0%"></div></div>
        <div class="progress-meta">
          <span id="progressLabel">{{if .Job}}{{.Job.Message}}{{else}}Listo para exportar{{end}}</span>
          <span id="progressPct">{{if .Job}}{{.Job.Progress}}{{else}}0{{end}}%</span>
        </div>
        <div class="phase-list" id="phaseList">
          <div class="phase" data-phase="queued">En cola</div>
          <div class="phase" data-phase="connecting">Conectando</div>
          <div class="phase" data-phase="inspecting">Inspeccionando</div>
          <div class="phase" data-phase="exporting">Exportando</div>
          <div class="phase" data-phase="finalizing">Finalizando</div>
        </div>
      </div>
      <div class="list">
        <div class="pill">1. Inspeccionar la consulta e inferir tipos</div>
        <div class="pill">2. Exportar a Parquet</div>
        <div class="pill">3. Validar la salida y previsualizar resultados</div>
        <div class="pill">4. Guardar como patrón repetible</div>
      </div>
      <div style="height:14px"></div>
      <div class="notice">Descubrimiento de tipos, tamaño de salida y validación aparecen antes y después de cada ejecución.</div>
    </div>

    <div class="section">
      <div class="section-title"><h2>Nueva exportación</h2><span>Formulario precargado desde entorno</span></div>
      {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}
        <div class="notice">Hay una exportación en curso. El formulario queda bloqueado hasta que termine.</div>
      {{end}}
      <form method="post" action="/export">
        <div class="grid2">
          <div><label>Usuario</label><input name="user" value="{{.Defaults.User}}" autocomplete="off" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}></div>
          <div><label>Clave</label><input name="pass" value="{{.Defaults.Pass}}" type="password" autocomplete="off" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}></div>
        </div>
        <div style="height:12px"></div>
        <div><label>Cadena de conexión</label><input name="connstr" value="{{.Defaults.ConnStr}}" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}></div>
        <div style="height:12px"></div>
        <div class="grid2">
          <div><label>Host</label><input name="host" value="{{.Defaults.Host}}" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}></div>
          <div><label>Servicio</label><input name="service" value="{{.Defaults.Service}}" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}></div>
        </div>
        <div style="height:12px"></div>
        <div class="grid2">
          <div><label>Port</label><input name="port" value="{{.Defaults.Port}}" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}></div>
          <div><label>Tiempo límite</label><input name="timeout" value="{{.Defaults.Timeout}}" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}></div>
        </div>
        <div style="height:12px"></div>
        <div><label>Archivo de salida</label><input name="out" value="{{.Defaults.Out}}" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}></div>
        <div style="height:12px"></div>
        <div><label>Consulta SQL</label><textarea name="query" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}>{{.Defaults.Query}}</textarea></div>
        <div style="height:10px"></div>
        <div class="notice">El archivo se escribe en el servidor. Al terminar, puedes descargarlo directamente desde el navegador.</div>
        <div style="height:14px"></div>
        <div class="row">
          <button class="btn primary" type="submit" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}>Ejecutar exportación</button>
          <button class="btn secondary" type="button" onclick="document.querySelector('textarea[name=query]').focus()" {{if and .Job (ne .Job.Status "completed") (ne .Job.Status "failed")}}disabled{{end}}>Editar consulta</button>
          {{if .DownloadAvailable}}<a class="btn secondary link" href="/download">Descargar Parquet</a>{{end}}
        </div>
      </form>
    </div>

    <div class="section">
      <div class="section-title"><h2>Último resultado</h2><span>Resumen en vivo de la salida</span></div>
      {{if .LastError}}<div class="error">{{.LastError}}</div>{{end}}
      {{if .LastResult}}
        <table class="table">
          <tr><th>Filas</th><td>{{.LastResult.Rows}}</td></tr>
          <tr><th>Tamaño</th><td>{{humanBytes .LastResult.FileSizeBytes}}</td></tr>
          <tr><th>Tiempo de consulta</th><td>{{humanDur .LastResult.QueryTime}}</td></tr>
          <tr><th>Tiempo de escritura</th><td>{{humanDur .LastResult.WriteTime}}</td></tr>
          <tr><th>Total</th><td>{{humanDur .LastResult.TotalTime}}</td></tr>
          <tr><th>Velocidad</th><td>{{printf "%.2f filas/segundo" .LastResult.Speed}}</td></tr>
          <tr><th>Mezcla de tipos</th><td>{{.LastResult.TypeMix}}</td></tr>
        </table>
            <div style="height:12px"></div>
            <div class="row">
              <a class="btn primary link" href="/download">Descargar último Parquet</a>
            </div>
      {{else}}
        <div class="notice">Todavía no se ha ejecutado ninguna exportación desde la interfaz web. Ejecuta una para desbloquear el panel de resultados.</div>
      {{end}}
    </div>

    <div class="section">
      <div class="section-title"><h2>Señales premium</h2><span>Lo que hace que se sienta valioso</span></div>
      <div class="list">
        <div class="pill">Descubrimiento de esquema antes de ejecutar</div>
        <div class="pill">Confianza y riesgo visibles desde el inicio</div>
        <div class="pill">Reejecución con un clic desde el mismo patrón</div>
        <div class="pill">Salida lista para producción con validación</div>
      </div>
    </div>
  </div>
  <script>
    async function refreshJobState() {
      try {
        const res = await fetch('/status', { cache: 'no-store' });
        if (!res.ok) return;
        const job = await res.json();
        const fill = document.getElementById('progressFill');
        const label = document.getElementById('progressLabel');
        const pct = document.getElementById('progressPct');
        const phaseList = document.getElementById('phaseList');
        if (fill) fill.style.width = (job.progress || 0) + '%';
        if (label) label.textContent = job.message || 'Listo para exportar';
        if (pct) pct.textContent = (job.progress || 0) + '%';
        if (phaseList && Array.isArray(job.phases)) {
          const nodes = phaseList.querySelectorAll('.phase');
          nodes.forEach((node) => {
            const phase = node.getAttribute('data-phase');
            const current = job.phases.find((p) => p.key === phase);
            node.classList.toggle('active', !!(current && current.active));
            node.classList.toggle('done', !!(current && current.completed));
          });
        }
        const downloadLinks = document.querySelectorAll('a[href="/download"]');
        downloadLinks.forEach((link) => {
          link.style.display = job.status === 'completed' ? 'inline-flex' : 'none';
        });
      } catch (err) {}
    }
    setInterval(refreshJobState, 1200);
    refreshJobState();
  </script>
</body>
</html>`

type exportPlan struct {
	User       string
	Target     string
	Query      string
	QueryBrief string
	Source     string
	Timeout    time.Duration
}

func buildExportPlan(user, connStr, host string, port int, service, query, out string, timeout time.Duration) exportPlan {
	source := "Oracle descriptor"
	if strings.TrimSpace(connStr) == "" {
		source = fmt.Sprintf("%s:%d/%s", host, port, service)
	}

	return exportPlan{
		User:       user,
		Target:     out,
		Query:      query,
		QueryBrief: summarizeQuery(query, 120),
		Source:     source,
		Timeout:    timeout,
	}
}

func renderLaunchScreen(plan exportPlan) {
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    DEJEMEVIVIR EXPORTER                    ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Source   : %-50s║\n", trimForPanel(plan.Source, 50))
	fmt.Printf("║ Target   : %-50s║\n", trimForPanel(plan.Target, 50))
	fmt.Printf("║ Timeout  : %-50s║\n", trimForPanel(plan.Timeout.Truncate(time.Second).String(), 50))
	fmt.Printf("║ User     : %-50s║\n", trimForPanel(plan.User, 50))
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Intent   : %-50s║\n", trimForPanel("Export query to trusted Parquet with live validation", 50))
	fmt.Printf("║ Query    : %-50s║\n", trimForPanel(plan.QueryBrief, 50))
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Focus")
	fmt.Println("  - Connect")
	fmt.Println("  - Inspect schema")
	fmt.Println("  - Export safely")
	fmt.Println("  - Validate output")
	fmt.Println()
}

func renderDiscoveryPanel(columns []string, specs []columnSpec) {
	fmt.Println("┌──────────────────────── Schema Discovery ────────────────────────┐")
	fmt.Printf("│ Columns detected: %-5d  Typed columns: %-5d  Risk: %-10s │\n", len(columns), len(specs), inferRiskLabel(specs))
	fmt.Println("├───────────────────────────────────────────────────────────────────┤")
	limit := len(columns)
	if limit > 8 {
		limit = 8
	}
	for i := 0; i < limit; i++ {
		fmt.Printf("│ %-24s → %-16s │\n", trimForPanel(columns[i], 24), columnKindLabel(specs[i].Kind))
	}
	if len(columns) > limit {
		fmt.Printf("│ ... and %-50s│\n", fmt.Sprintf("%d more columns", len(columns)-limit))
	}
	fmt.Println("└───────────────────────────────────────────────────────────────────┘")
	fmt.Println()
}

func renderSchemaPanel(schemaJSON string, specs []columnSpec, parquetNames []string) {
	fmt.Println("┌──────────────────────── Export Blueprint ────────────────────────┐")
	fmt.Printf("│ Schema stability: %-47s│\n", schemaStabilityText(specs))
	fmt.Printf("│ Output columns  : %-47d│\n", len(parquetNames))
	fmt.Println("├───────────────────────────────────────────────────────────────────┤")
	for i := 0; i < len(parquetNames) && i < 6; i++ {
		fmt.Printf("│ %-24s → %-16s │\n", trimForPanel(parquetNames[i], 24), columnKindLabel(specs[i].Kind))
	}
	if len(parquetNames) > 6 {
		fmt.Printf("│ ... and %-50s│\n", fmt.Sprintf("%d more mapped columns", len(parquetNames)-6))
	}
	fmt.Println("└───────────────────────────────────────────────────────────────────┘")
	_ = schemaJSON
	fmt.Println()
}

func renderSuccessPanel(out string, fileSizeBytes int64, count int64, queryStart, writeStart time.Time, writeTime, totalTime time.Duration, columns int, specs []columnSpec) {
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    EXPORT COMPLETED                         ║")
	fmt.Println("╠════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ File     : %-50s║\n", trimForPanel(out, 50))
	fmt.Printf("║ Size     : %-50s║\n", trimForPanel(formatBytes(fileSizeBytes), 50))
	fmt.Printf("║ Rows     : %-50d║\n", count)
	fmt.Printf("║ Columns  : %-50d║\n", columns)
	fmt.Printf("║ Query    : %-50s║\n", trimForPanel(queryElapsed(queryStart).String(), 50))
	fmt.Printf("║ Write    : %-50s║\n", trimForPanel(writeTime.Truncate(time.Millisecond).String(), 50))
	fmt.Printf("║ Total    : %-50s║\n", trimForPanel(totalTime.Truncate(time.Millisecond).String(), 50))
	fmt.Printf("║ Speed    : %-50s║\n", trimForPanel(fmt.Sprintf("%.2f rows/sec", rowsPerSecond(count, writeStart)), 50))
	fmt.Printf("║ Type mix : %-50s║\n", trimForPanel(summarizeTypeMix(specs), 50))
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()
}

func newExportJob(defaults exportDefaults) *exportJobView {
	now := time.Now()
	return &exportJobView{
		ID:       strconv.FormatInt(now.UnixNano(), 10),
		Status:   jobQueued,
		Message:  "En cola",
		Progress: 0,
		Phases: []exportJobPhase{
			{Key: "queued", Label: "En cola", Percent: 0, Active: true},
			{Key: "connecting", Label: "Conectando", Percent: 12},
			{Key: "inspecting", Label: "Inspeccionando", Percent: 28},
			{Key: "exporting", Label: "Exportando", Percent: 72},
			{Key: "finalizing", Label: "Finalizando", Percent: 90},
			{Key: "done", Label: "Listo", Percent: 100},
		},
		StartedAt:  now,
		UpdatedAt:  now,
	}
}

func updateJobProgress(job *exportJobView, phase string, percent int, message string) {
	if job == nil {
		return
	}
	job.Status = jobRunning
	job.Progress = clampProgress(percent)
	job.Message = message
	job.UpdatedAt = time.Now()
	for i := range job.Phases {
		job.Phases[i].Active = job.Phases[i].Key == phase
		job.Phases[i].Completed = job.Phases[i].Percent <= job.Progress && job.Phases[i].Key != phase
	}
}

func completeJob(job *exportJobView, result exportResult) {
	if job == nil {
		return
	}
	job.Status = jobCompleted
	job.Progress = 100
	job.Message = "Exportación completada"
	job.Result = &result
	job.DownloadURL = "/download"
	job.FinishedAt = time.Now()
	job.UpdatedAt = job.FinishedAt
	for i := range job.Phases {
		job.Phases[i].Active = job.Phases[i].Key == "done"
		job.Phases[i].Completed = true
	}
}

func failJob(job *exportJobView, err error) {
	if job == nil {
		return
	}
	job.Status = jobFailed
	job.Progress = clampProgress(job.Progress)
	job.Message = "La exportación falló"
	job.Error = err.Error()
	job.FinishedAt = time.Now()
	job.UpdatedAt = job.FinishedAt
}

func jobResult(job *exportJobView) *exportResult {
	if job == nil {
		return nil
	}
	return job.Result
}

func jobError(job *exportJobView) string {
	if job == nil {
		return ""
	}
	return job.Error
}

func clampProgress(n int) int {
	if n < 0 {
		return 0
	}
	if n > 100 {
		return 100
	}
	return n
}

func summarizeTypeMix(specs []columnSpec) string {
	counts := map[columnKind]int{}
	for _, s := range specs {
		counts[s.Kind]++
	}
	return fmt.Sprintf("ts %d | bool %d | int %d | float %d | bin %d | text %d",
		counts[columnKindTimestampMicros],
		counts[columnKindBool],
		counts[columnKindInt64],
		counts[columnKindFloat64],
		counts[columnKindBinary],
		counts[columnKindString],
	)
}

func schemaStabilityText(specs []columnSpec) string {
	if len(specs) == 0 {
		return "unknown"
	}
	safe := 0
	for _, s := range specs {
		if s.Kind != columnKindString {
			safe++
		}
	}
	pct := (safe * 100) / len(specs)
	switch {
	case pct >= 80:
		return fmt.Sprintf("high (%d%% typed)", pct)
	case pct >= 50:
		return fmt.Sprintf("medium (%d%% typed)", pct)
	default:
		return fmt.Sprintf("low (%d%% typed)", pct)
	}
}

func inferRiskLabel(specs []columnSpec) string {
	if len(specs) == 0 {
		return "unknown"
	}
	for _, s := range specs {
		if s.Kind == columnKindBinary {
			return "binary"
		}
	}
	return "low"
}

func columnKindLabel(kind columnKind) string {
	switch kind {
	case columnKindTimestampMicros:
		return "timestamp"
	case columnKindBool:
		return "bool"
	case columnKindInt64:
		return "int64"
	case columnKindFloat64:
		return "float64"
	case columnKindBinary:
		return "binary"
	default:
		return "text"
	}
}

func summarizeQuery(query string, max int) string {
	query = strings.Join(strings.Fields(query), " ")
	if len(query) <= max {
		return query
	}
	if max < 4 {
		return query[:max]
	}
	return query[:max-3] + "..."
}

func trimForPanel(value string, max int) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.TrimSpace(value)
	if len(value) <= max {
		return value
	}
	if max < 4 {
		return value[:max]
	}
	return value[:max-3] + "..."
}
