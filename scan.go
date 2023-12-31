package gorm

import (
	"database/sql"
	"database/sql/driver"
	"reflect"
	"time"

	"gorm.io/gorm/schema"
	"gorm.io/gorm/utils"
)

// prepareValues prepare values slice
func prepareValues(values []interface{}, db *DB, columnTypes []*sql.ColumnType, columns []string) {
	if db.Statement.Schema != nil {
		for idx, name := range columns {
			if field := db.Statement.Schema.LookUpField(name); field != nil {
				values[idx] = reflect.New(reflect.PtrTo(field.FieldType)).Interface()
				continue
			}
			values[idx] = new(interface{})
		}
	} else if len(columnTypes) > 0 {
		for idx, columnType := range columnTypes {
			if columnType.ScanType() != nil {
				values[idx] = reflect.New(reflect.PtrTo(columnType.ScanType())).Interface()
			} else {
				values[idx] = new(interface{})
			}
		}
	} else {
		for idx := range columns {
			values[idx] = new(interface{})
		}
	}
}

func scanIntoMap(mapValue map[string]interface{}, values []interface{}, columns []string) {
	for idx, column := range columns {
		if reflectValue := reflect.Indirect(reflect.Indirect(reflect.ValueOf(values[idx]))); reflectValue.IsValid() {
			mapValue[column] = reflectValue.Interface()
			if valuer, ok := mapValue[column].(driver.Valuer); ok {
				mapValue[column], _ = valuer.Value()
			} else if b, ok := mapValue[column].(sql.RawBytes); ok {
				mapValue[column] = string(b)
			}
		} else {
			mapValue[column] = nil
		}
	}
}

func (db *DB) scanIntoStruct(rows Rows, reflectValue reflect.Value, values []interface{}, fields []*schema.Field, joinFields [][]*schema.Field) {
	for idx, field := range fields {
		if field != nil {
			values[idx] = field.NewValuePool.Get() // 从对象池里面 new 一个对象
		} else if len(fields) == 1 {
			if reflectValue.CanAddr() {
				values[idx] = reflectValue.Addr().Interface()
			} else {
				values[idx] = reflectValue.Interface()
			}
		}
	}

	db.RowsAffected++
	db.AddError(rows.Scan(values...))
	joinedNestedSchemaMap := make(map[string]interface{})
	for idx, field := range fields {
		if field == nil {
			continue
		}

		if len(joinFields) == 0 || len(joinFields[idx]) == 0 {
			db.AddError(field.Set(db.Statement.Context, reflectValue, values[idx]))
		} else { // joinFields count is larger than 2 when using join
			var isNilPtrValue bool
			var relValue reflect.Value
			// does not contain raw dbname
			nestedJoinSchemas := joinFields[idx][:len(joinFields[idx])-1]
			// current reflect value
			currentReflectValue := reflectValue
			fullRels := make([]string, 0, len(nestedJoinSchemas))
			for _, joinSchema := range nestedJoinSchemas {
				fullRels = append(fullRels, joinSchema.Name)
				relValue = joinSchema.ReflectValueOf(db.Statement.Context, currentReflectValue)
				if relValue.Kind() == reflect.Ptr {
					fullRelsName := utils.JoinNestedRelationNames(fullRels)
					// same nested structure
					if _, ok := joinedNestedSchemaMap[fullRelsName]; !ok {
						if value := reflect.ValueOf(values[idx]).Elem(); value.Kind() == reflect.Ptr && value.IsNil() {
							isNilPtrValue = true
							break
						}

						relValue.Set(reflect.New(relValue.Type().Elem()))
						joinedNestedSchemaMap[fullRelsName] = nil
					}
				}
				currentReflectValue = relValue
			}

			if !isNilPtrValue { // ignore if value is nil
				f := joinFields[idx][len(joinFields[idx])-1]
				db.AddError(f.Set(db.Statement.Context, relValue, values[idx]))
			}
		}

		// release data to pool
		field.NewValuePool.Put(values[idx]) // 放回对象池
	}
}

// ScanMode scan data mode
type ScanMode uint8

// scan modes
const (
	ScanInitialized         ScanMode = 1 << 0 // 1
	ScanUpdate              ScanMode = 1 << 1 // 2
	ScanOnConflictDoNothing ScanMode = 1 << 2 // 4
)

// Scan scan rows into db statement
func Scan(rows Rows, db *DB, mode ScanMode) {
	var (
		columns, _          = rows.Columns() // 所有的列集合
		values              = make([]interface{}, len(columns))
		initialized         = mode&ScanInitialized != 0
		update              = mode&ScanUpdate != 0
		onConflictDonothing = mode&ScanOnConflictDoNothing != 0
	)

	db.RowsAffected = 0

	switch dest := db.Statement.Dest.(type) { // switch 要 scan 的目标
	case map[string]interface{}, *map[string]interface{}: // 如果是要 scan 到 map 里面
		if initialized || rows.Next() {
			columnTypes, _ := rows.ColumnTypes()
			prepareValues(values, db, columnTypes, columns)

			db.RowsAffected++
			db.AddError(rows.Scan(values...))

			mapValue, ok := dest.(map[string]interface{})
			if !ok {
				if v, ok := dest.(*map[string]interface{}); ok {
					if *v == nil {
						*v = map[string]interface{}{}
					}
					mapValue = *v
				}
			}
			scanIntoMap(mapValue, values, columns)
		}
	case *[]map[string]interface{}: // 如果是要 scan 到 []map 里面
		columnTypes, _ := rows.ColumnTypes()
		for initialized || rows.Next() {
			prepareValues(values, db, columnTypes, columns)

			initialized = false
			db.RowsAffected++
			db.AddError(rows.Scan(values...))

			mapValue := map[string]interface{}{}
			scanIntoMap(mapValue, values, columns)
			*dest = append(*dest, mapValue)
		}
	case *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64, *uintptr,
		*float32, *float64,
		*bool, *string, *time.Time,
		*sql.NullInt32, *sql.NullInt64, *sql.NullFloat64,
		*sql.NullBool, *sql.NullString, *sql.NullTime:
		for initialized || rows.Next() {
			initialized = false
			db.RowsAffected++
			db.AddError(rows.Scan(dest))
		}
	default: // 结构体
		var (
			fields       = make([]*schema.Field, len(columns))
			joinFields   [][]*schema.Field
			sch          = db.Statement.Schema
			reflectValue = db.Statement.ReflectValue // dest 的值的 reflect.Value
		)

		if reflectValue.Kind() == reflect.Interface {
			reflectValue = reflectValue.Elem() // 如果是接口，取实际的值
		}

		reflectValueType := reflectValue.Type()
		switch reflectValueType.Kind() {
		case reflect.Array, reflect.Slice:
			reflectValueType = reflectValueType.Elem() // 如果是 list, 取元素类型
		}
		isPtr := reflectValueType.Kind() == reflect.Ptr // dest 或者其元素是否是指针
		if isPtr {
			reflectValueType = reflectValueType.Elem() // 如果是指针，取结构体
		}

		if sch != nil {
			// 如果 db.Statement.Schema 已经解析过了，判断是否 dest 和 model 的类型一样，
			// 如果不一样，并且 dest 是结构体，会解析一下 dest 的 schema
			if reflectValueType != sch.ModelType && reflectValueType.Kind() == reflect.Struct {
				sch, _ = schema.Parse(db.Statement.Dest, db.cacheStore, db.NamingStrategy)
			}

			if len(columns) == 1 { // 查询单个列
				// Is Pluck
				if _, ok := reflect.New(reflectValueType).Interface().(sql.Scanner); (reflectValueType != sch.ModelType && ok) || // is scanner
					reflectValueType.Kind() != reflect.Struct || // is not struct
					sch.ModelType.ConvertibleTo(schema.TimeReflectType) { // is time
					sch = nil
					// 以下情况不需要 schema
					// - dest 和 model 不是同类型, 并且 dest 实现了 sql.Scanne 接口
					// - dest 解引用后后最终不是结构体
					// - dest 是 time.Time 及其衍生类型
				}
			}

			// Not Pluck
			// 这个时候还有 schema， 说明是结构体字段接收列值，不是 pluck 那种，整个 dest 接收一个列
			if sch != nil {
				matchedFieldCount := make(map[string]int, len(columns))
				for idx, column := range columns { // 遍历 db 返回接口的所有列的名字
					if field := sch.LookUpField(column); field != nil && field.Readable { // 如果当前字段能从 schema里面取到 Field, 并且可读
						fields[idx] = field
						if count, ok := matchedFieldCount[column]; ok {
							// 如果 db 返回结果里面的某个字段之前已经匹配到了一个 field，说明 columns 里面有重复字段
							// handle duplicate fields
							for _, selectField := range sch.Fields { // 遍历 schema 里面的所有 fields
								if selectField.DBName == column && selectField.Readable { // 如果 dbName 精确匹配到了
									if count == 0 {
										matchedFieldCount[column]++
										fields[idx] = selectField
										break // 取匹配到的第 count 个 field
									}
									count-- // 之前的 field 已经匹配到了，跳过
								}
							}
						} else {
							matchedFieldCount[column] = 1
						}
					} else if names := utils.SplitNestedRelationName(column); len(names) > 1 { // has nested relation
						if rel, ok := sch.Relationships.Relations[names[0]]; ok {
							subNameCount := len(names)
							// nested relation fields
							relFields := make([]*schema.Field, 0, subNameCount-1)
							relFields = append(relFields, rel.Field)
							for _, name := range names[1 : subNameCount-1] {
								rel = rel.FieldSchema.Relationships.Relations[name]
								relFields = append(relFields, rel.Field)
							}
							// lastest name is raw dbname
							dbName := names[subNameCount-1]
							if field := rel.FieldSchema.LookUpField(dbName); field != nil && field.Readable {
								fields[idx] = field

								if len(joinFields) == 0 {
									joinFields = make([][]*schema.Field, len(columns))
								}
								relFields = append(relFields, field)
								joinFields[idx] = relFields
								continue
							}
						}
						values[idx] = &sql.RawBytes{}
					} else {
						values[idx] = &sql.RawBytes{}
					}
				}
			}
		}

		switch reflectValue.Kind() { // switch dest 的值的 reflect.Value
		case reflect.Slice, reflect.Array: // dset 是数组或者切片
			var (
				elem        reflect.Value
				isArrayKind = reflectValue.Kind() == reflect.Array // 是否是数组
			)

			if !update || reflectValue.Len() == 0 {
				update = false
				// if the slice cap is externally initialized, the externally initialized slice is directly used here
				if reflectValue.Cap() == 0 { // 如果容量是0，扩容到 20
					db.Statement.ReflectValue.Set(reflect.MakeSlice(reflectValue.Type(), 0, 20))
				} else if !isArrayKind { // 如果不是数组， 将长度设置为 0
					reflectValue.SetLen(0)
					db.Statement.ReflectValue.Set(reflectValue)
				}
			}

			for initialized || rows.Next() {
			BEGIN:
				initialized = false

				if update {
					if int(db.RowsAffected) >= reflectValue.Len() {
						return
					}
					elem = reflectValue.Index(int(db.RowsAffected))
					if onConflictDonothing {
						for _, field := range fields {
							if _, ok := field.ValueOf(db.Statement.Context, elem); !ok {
								db.RowsAffected++
								goto BEGIN
							}
						}
					}
				} else {
					elem = reflect.New(reflectValueType)
				}

				db.scanIntoStruct(rows, elem, values, fields, joinFields)

				if !update {
					if !isPtr { // 如果是指针，取其元素
						elem = elem.Elem()
					}
					if isArrayKind { // 如果是数组，
						if reflectValue.Len() >= int(db.RowsAffected) { // 如果当前元素不超过数组长度
							reflectValue.Index(int(db.RowsAffected - 1)).Set(elem) // 设置第 db.RowsAffected - 1 个元素
						}
					} else {
						reflectValue = reflect.Append(reflectValue, elem) // 切片追加到最后
					}
				}
			}

			if !update {
				db.Statement.ReflectValue.Set(reflectValue)
			}
		case reflect.Struct, reflect.Ptr:
			if initialized || rows.Next() {
				db.scanIntoStruct(rows, reflectValue, values, fields, joinFields)
			}
		default:
			db.AddError(rows.Scan(dest))
		}
	}

	if err := rows.Err(); err != nil && err != db.Error {
		db.AddError(err)
	}

	if db.RowsAffected == 0 && db.Statement.RaiseErrorOnNotFound && db.Error == nil {
		db.AddError(ErrRecordNotFound)
	}
}
