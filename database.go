package gsh

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
)

// database connect instance
var DB *sql.DB

// process sql error
var ProcessSqlError func(err error)

// initialize
func init() {
	ProcessSqlError = func(err error) {
		if err != nil {
			log.Println("Sql error: ", err.Error())
		}
	}
}

// execute insert one sql return id
func Add(query string, args ...interface{}) (id int64) {
	stmt, err := DB.Prepare(query)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	result, err := stmt.Exec(args...)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	id, err = result.LastInsertId()
	if err != nil {
		ProcessSqlError(err)
		return
	}
	return
}

// execute sql
func Exec(query string, args ...interface{}) (rows int64) {
	stmt, err := DB.Prepare(query)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	result, err := stmt.Exec(args...)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	rows, err = result.RowsAffected()
	if err != nil {
		ProcessSqlError(err)
		return
	}
	return
}

// query all sql
func Query(query string, args ...interface{}) (slices []map[string]interface{}) {
	stmt, err := DB.Prepare(query)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	rows, err := stmt.Query(args...)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	cols, err := rows.Columns()
	if err != nil {
		ProcessSqlError(err)
		return
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		ProcessSqlError(err)
		return
	}
	var lc int
	var vs, st string
	var x interface{}
	lc = len(cols)
	vals := make([][]byte, lc)
	scan := make([]interface{}, lc)
	for k, _ := range vals {
		scan[k] = &vals[k]
	}
	for rows.Next() {
		err = rows.Scan(scan...)
		if err != nil {
			ProcessSqlError(err)
			return
		}
		tmp := make(map[string]interface{})
		for k, v := range vals {
			if v == nil {
				tmp[cols[k]] = v
				continue
			}
			// []byte => string => other base type
			vs = string(v)
			st = cts[k].ScanType().String()
			switch st {
			case "int":
				x, err = strconv.Atoi(vs)
				if err != nil {
					ProcessSqlError(err)
					return
				}
				tmp[cols[k]] = x
			case "int8", "int16", "int32", "int64":
				x, err = strconv.ParseInt(vs, 10, 64)
				if err != nil {
					ProcessSqlError(err)
					return
				}
				tmp[cols[k]] = x
			case "uint", "uint8", "uint16", "uint32", "uint64":
				x, err = strconv.ParseUint(vs, 10, 64)
				if err != nil {
					ProcessSqlError(err)
					return
				}
				tmp[cols[k]] = x
			case "float32", "float64":
				x, err = strconv.ParseFloat(vs, 64)
				if err != nil {
					ProcessSqlError(err)
					return
				}
				tmp[cols[k]] = x
			case "string":
				tmp[cols[k]] = vs
			case "bool":
				x, err = strconv.ParseBool(vs)
				if err != nil {
					ProcessSqlError(err)
					return
				}
				tmp[cols[k]] = x
			case "sql.NullBool":
				x, err = strconv.ParseBool(vs)
				if err != nil {
					ProcessSqlError(err)
					return
				}
				tmp[cols[k]] = x
			case "sql.NullFloat64":
				x, err = strconv.ParseFloat(vs, 64)
				if err != nil {
					ProcessSqlError(err)
					return
				}
				tmp[cols[k]] = x
			case "sql.NullInt32", "sql.NullInt64":
				x, err = strconv.ParseInt(vs, 10, 64)
				if err != nil {
					ProcessSqlError(err)
					return
				}
				tmp[cols[k]] = x
			case "sql.NullString":
				tmp[cols[k]] = vs
			case "sql.NullTime":
				tmp[cols[k]] = vs
			case "sql.RawBytes":
				tmp[cols[k]] = vs
			default:
				tmp[cols[k]] = vs
			}
		}
		slices = append(slices, tmp)
	}
	return
}

// query one sql
func Get(get interface{}, query string, args ...interface{}) {
	err := errors.New("Parameter must be a structure pointer!")
	t, v := reflect.TypeOf(get), reflect.ValueOf(get)
	if t.Kind() != reflect.Ptr {
		ProcessSqlError(err)
		return
	}
	t, v = t.Elem(), v.Elem()
	if t.Kind() != reflect.Struct {
		ProcessSqlError(err)
		return
	}
	err = nil
	stmt, err := DB.Prepare(query)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	rows, err := stmt.Query(args...)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	columns, err := rows.Columns()
	if err != nil {
		ProcessSqlError(err)
		return
	}
	field := reflect.Value{}
	reflectZeroValue := reflect.Value{}
	result := reflect.New(t)
	resultValue := reflect.Indirect(result)
	scans := []interface{}{}
	scanned := false
	for rows.Next() {
		if scanned {
			break
		}
		for _, column := range columns {
			column = UnderlineToPascal(column)
			field = resultValue.FieldByName(column)
			if field == reflectZeroValue {
				ProcessSqlError(errors.New(fmt.Sprintf("Can not find the corresponding field <%s> found in the structure <%s>!", column, t.Name())))
				return
			}
			if !field.CanSet() {
				ProcessSqlError(errors.New(fmt.Sprintf("Unable to set value, corresponding field <%s> was found in structure <%s>!", column, t.Name())))
				return
			}
			scans = append(scans, field.Addr().Interface())
		}
		err = rows.Scan(scans...)
		if err != nil {
			ProcessSqlError(err)
			return
		}
		scanned = true
	}
	reflect.ValueOf(get).Elem().Set(result.Elem())
	return
}

// query all sql
func GetAll(get interface{}, query string, args ...interface{}) {
	stmt, err := DB.Prepare(query)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	rows, err := stmt.Query(args...)
	if err != nil {
		ProcessSqlError(err)
		return
	}
	t := reflect.TypeOf(get)
	k := t.Kind()
	if reflect.Ptr != k {
		ProcessSqlError(errors.New("The parameter to receive data is not a pointer, require *[]struct or *[]*struct!"))
		return
	}
	k = t.Elem().Kind()
	if reflect.Slice != k {
		ProcessSqlError(errors.New("The parameter to receive data is not a pointer, require *[]struct or *[]*struct!"))
		return
	}
	k = t.Elem().Elem().Kind()
	// reflect zero value
	reflectZeroValue := reflect.Value{}
	switch k {
	// *[]struct
	case reflect.Struct:
		columns, err := rows.Columns()
		if err != nil {
			ProcessSqlError(err)
			return
		}
		// []struct
		result := reflect.ValueOf(get).Elem()
		// reflect zero value
		reflectZeroValue := reflect.Value{}
		// new struct type (pointer)
		rowResult := reflect.New(t.Elem().Elem())
		// get new struct value
		rowResultValue := reflect.Indirect(rowResult)
		// rows scan columns list
		scans := []interface{}{}
		for _, column := range columns {
			filed := rowResultValue.FieldByName(UnderlineToPascal(column))
			if reflectZeroValue == filed {
				ProcessSqlError(errors.New(fmt.Sprintf("Can not find the corresponding field <%s> found in the structure <%s>!", column, t.Elem().Elem().Name())))
				return
			}
			if !filed.CanSet() {
				ProcessSqlError(errors.New(fmt.Sprintf("Unable to set value, corresponding field <%s> was found in structure <%s>!", column, t.Elem().Elem().Name())))
				return
			}
			scans = append(scans, filed.Addr().Interface())
		}
		for rows.Next() {
			err := rows.Scan(scans...)
			if err != nil {
				ProcessSqlError(err)
				return
			}
			result = reflect.Append(result, rowResult.Elem())
		}
		reflect.ValueOf(get).Elem().Set(result)
	// *[]*struct
	case reflect.Ptr:
		if reflect.Struct != t.Elem().Elem().Elem().Kind() {
			ProcessSqlError(errors.New("The parameter to receive data is not a pointer, require *[]struct or *[]*struct!"))
			return
		}
		columns, err := rows.Columns()
		if err != nil {
			ProcessSqlError(err)
			return
		}
		// []*struct
		result := reflect.ValueOf(get).Elem()
		var rowResult reflect.Value
		var rowResultValue reflect.Value
		var scans []interface{}
		for rows.Next() {
			// new struct type (pointer)
			rowResult = reflect.New(t.Elem().Elem().Elem())
			// get new struct value
			rowResultValue = reflect.Indirect(rowResult)
			// rows scan columns list
			scans = []interface{}{}
			for _, column := range columns {
				filed := rowResultValue.FieldByName(UnderlineToPascal(column))
				if reflectZeroValue == filed {
					ProcessSqlError(errors.New(fmt.Sprintf("Can not find the corresponding field <%s> found in the structure <%s>!", column, t.Elem().Elem().Elem().Name())))
					return
				}
				if !filed.CanSet() {
					ProcessSqlError(errors.New(fmt.Sprintf("Unable to set value, corresponding field <%s> was found in structure <%s>!", column, t.Elem().Elem().Elem().Name())))
					return
				}
				scans = append(scans, filed.Addr().Interface())
			}
			err = rows.Scan(scans...)
			if err != nil {
				ProcessSqlError(err)
				return
			}
			result = reflect.Append(result, rowResult)
		}
		reflect.ValueOf(get).Elem().Set(result)
	default:
		ProcessSqlError(errors.New("The parameter to receive data is not a pointer, require *[]struct or *[]*struct!"))
		return
	}
	return
}
