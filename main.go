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
	"log"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
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

	if strings.TrimSpace(*user) == "" || strings.TrimSpace(*pass) == "" {
		return fmt.Errorf("faltan credenciales: usa -user y -pass o define ORACLE_USER y ORACLE_PASSWORD")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	startTime := time.Now()

	fmt.Println("==================================================")
	fmt.Println("                 DEJEMEVIVIR PARQUET               ")
	fmt.Println("==================================================")
	fmt.Printf("Autor: %s\n", softwareAuthor)
	fmt.Println("Propiedad intelectual de este software: Christian Reinaldo Ruiz Buitron")
	fmt.Println()

	fmt.Println(" -> Conectando a Oracle...")
	connectStart := time.Now()
	db, err := openOracle(*connStr, *host, *port, *service, *user, *pass)
	if err != nil {
		return fmt.Errorf("error conectando a Oracle: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("no se pudo hacer ping a Oracle: %w", err)
	}
	fmt.Printf(" -> Conexión establecida en %v\n", time.Since(connectStart).Truncate(time.Millisecond))

	fmt.Println(" -> Ejecutando consulta...")
	queryStart := time.Now()
	rows, err := db.QueryContext(ctx, *query)
	if err != nil {
		return fmt.Errorf("error ejecutando query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("no se pudieron leer columnas: %w", err)
	}
	fmt.Printf(" -> Query respondida en %v. Columnas detectadas: %d\n",
		time.Since(queryStart).Truncate(time.Millisecond), len(columns))

	columnSpecs := inferColumnSpecsTyped(rows, columns)
	schemaJSON, parquetNames, err := buildSchemaTyped(columnSpecs)
	if err != nil {
		return err
	}

	f, err := os.Create(*out)
	if err != nil {
		return fmt.Errorf("no se pudo crear archivo de salida: %w", err)
	}
	pw, err := writer.NewJSONWriter(schemaJSON, writerfile.NewWriterFile(f), 4)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("no se pudo crear writer parquet: %w", err)
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
			return fmt.Errorf("error leyendo fila: %w", err)
		}

		payload := make(map[string]any, len(columns))
		for i, col := range parquetNames {
			converted, err := valueToJSONTyped(values[i], columnSpecs[i].Kind)
			if err != nil {
				_ = pw.WriteStop()
				_ = f.Close()
				return fmt.Errorf("error convirtiendo columna %s: %w", col, err)
			}
			payload[col] = converted
		}

		rec, err := json.Marshal(payload)
		if err != nil {
			_ = pw.WriteStop()
			_ = f.Close()
			return fmt.Errorf("error serializando fila a JSON: %w", err)
		}
		if err := pw.Write(string(rec)); err != nil {
			_ = pw.WriteStop()
			_ = f.Close()
			return fmt.Errorf("error escribiendo parquet: %w", err)
		}

		count++
		if count == 1 || count%1000 == 0 {
			rate := rowsPerSecond(count, writeStart)
			fmt.Printf("\r -> Procesadas %d filas (%.2f filas/segundo)", count, rate)
		}
	}

	if count > 0 {
		fmt.Println()
	}
	if err := rows.Err(); err != nil {
		_ = pw.WriteStop()
		_ = f.Close()
		return fmt.Errorf("error iterando filas: %w", err)
	}
	if err := pw.WriteStop(); err != nil {
		_ = f.Close()
		return fmt.Errorf("error cerrando parquet: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("no se pudo cerrar el archivo de salida: %w", err)
	}

	writeTime := time.Since(writeStart)
	totalTime := time.Since(startTime)
	fileInfo, statErr := os.Stat(*out)
	var fileSizeBytes int64
	if statErr == nil {
		fileSizeBytes = fileInfo.Size()
	}

	fmt.Println("==================================================")
	fmt.Println("               PROCESAMIENTO EXITOSO              ")
	fmt.Println("==================================================")
	fmt.Printf(" * Autor:              %s\n", softwareAuthor)
	fmt.Println(" * Propiedad intelectual: Christian Reinaldo Ruiz Buitron")
	fmt.Printf(" * Archivo generado:   %s\n", *out)
	fmt.Printf(" * Tamaño del archivo: %s\n", formatBytes(fileSizeBytes))
	fmt.Printf(" * Filas procesadas:   %d\n", count)
	fmt.Printf(" * Tiempo de consulta: %v\n", queryElapsed(queryStart))
	fmt.Printf(" * Tiempo de escritura:%v\n", writeTime.Truncate(time.Millisecond))
	fmt.Printf(" * Velocidad media:    %.2f filas/segundo\n", rowsPerSecond(count, writeStart))
	fmt.Printf(" * Tiempo total:       %v\n", totalTime.Truncate(time.Millisecond))
	fmt.Println()

	fmt.Println("==================================================")
	fmt.Println("      INSPECCIÓN EN VIVO: PRIMERAS 3 FILAS        ")
	fmt.Println("==================================================")
	if err := previewParquetRows(*out, 3); err != nil {
		fmt.Printf("⚠ No se pudo leer la vista previa del Parquet: %v\n", err)
	}
	return nil
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
