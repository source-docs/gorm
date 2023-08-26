package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/now"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/utils"
)

// special types' reflect type
var (
	TimeReflectType    = reflect.TypeOf(time.Time{})
	TimePtrReflectType = reflect.TypeOf(&time.Time{})
	ByteReflectType    = reflect.TypeOf(uint8(0))
)

type (
	// DataType GORM data type
	DataType string
	// TimeType GORM time type
	TimeType int64
)

// GORM time types
const (
	UnixTime        TimeType = 1
	UnixSecond      TimeType = 2
	UnixMillisecond TimeType = 3
	UnixNanosecond  TimeType = 4
)

// GORM fields types
const (
	Bool   DataType = "bool"
	Int    DataType = "int"
	Uint   DataType = "uint"
	Float  DataType = "float"
	String DataType = "string"
	Time   DataType = "time"
	Bytes  DataType = "bytes"
)

// Field is the representation of model schema's field
type Field struct {
	Name                   string              // 结构体的名字
	DBName                 string              // 结构体对应的 db COLUMN 名字
	BindNames              []string            // 带结构体层级的 Name, 然后是嵌套结构体，倒数第一个值是字段名，上一个值是上级结构体名
	DataType               DataType            // 表示数据库字段类型
	GORMDataType           DataType            // 用于处理数据库字段类型和 Golang 类型之间映射
	PrimaryKey             bool                // 该字段是否是主键
	AutoIncrement          bool                // 该字段是否自增
	AutoIncrementIncrement int64               // 自增开始值，用 AUTOINCREMENTINCREMENT 注解定义
	Creatable              bool                // 创建的时候可见
	Updatable              bool                // 更新的时候可见
	Readable               bool                // 读取的时候可见
	AutoCreateTime         TimeType            // 在创建的时候自动设置创建时间,及其设置形式
	AutoUpdateTime         TimeType            // 在创建和更新的时候自动设置更新时间,及其设置形式
	HasDefaultValue        bool                // 该字段是否有默认值，带有 default 注解，或者是自增的注解
	DefaultValue           string              // 该字段的默认值
	DefaultValueInterface  interface{}         // 解析后的默认值
	NotNull                bool                // 是否是 NOT NULL
	Unique                 bool                // 是否是唯一的
	Comment                string              // 表字段注释
	Size                   int                 // 字段的大小
	Precision              int                 // 精度
	Scale                  int                 // 小数位数的精度
	IgnoreMigration        bool                // migration 时忽略该字段
	FieldType              reflect.Type        // 字段的类型，可能是指针
	IndirectFieldType      reflect.Type        // 字段的真实类型
	StructField            reflect.StructField // 从当前字段所属结构体里面取出来的字段定义,如果是嵌套结构体，则 Index 会有多层
	Tag                    reflect.StructTag   // 字段的 tag
	TagSettings            map[string]string   // 从字段 gorm 注解里面解析出来的配置
	Schema                 *Schema             // 字段所属的 model 结构体的 schema, (最外层)
	EmbeddedSchema         *Schema             // 如果当前字段是一个嵌套结构体，其 Schema 保存在这里
	OwnerSchema            *Schema             // 嵌入的结构体解析出来的 Schema
	ReflectValueOf         func(context.Context, reflect.Value) reflect.Value
	// 该方法返回当前字段的 interface 值和是否是 zero, 如果当前 字段定义是嵌套结构体，会返回嵌套结构体的 Value
	ValueOf      func(context.Context, reflect.Value) (value interface{}, zero bool)
	Set          func(context.Context, reflect.Value, interface{}) error
	Serializer   SerializerInterface // 该字段配置的序列化器
	NewValuePool FieldNewValuePool
}

func (field *Field) BindName() string {
	return strings.Join(field.BindNames, ".")
}

// ParseField parses reflect.StructField to Field
func (schema *Schema) ParseField(fieldStruct reflect.StructField) *Field {
	var (
		err        error
		tagSetting = ParseTagSetting(fieldStruct.Tag.Get("gorm"), ";") // 解析当前字段的 gorm 注解到 tagSetting map 里面
	)

	field := &Field{
		Name:                   fieldStruct.Name,
		DBName:                 tagSetting["COLUMN"],
		BindNames:              []string{fieldStruct.Name},
		FieldType:              fieldStruct.Type,
		IndirectFieldType:      fieldStruct.Type,
		StructField:            fieldStruct,
		Tag:                    fieldStruct.Tag,
		TagSettings:            tagSetting,
		Schema:                 schema,
		Creatable:              true,
		Updatable:              true,
		Readable:               true,
		PrimaryKey:             utils.CheckTruth(tagSetting["PRIMARYKEY"], tagSetting["PRIMARY_KEY"]),
		AutoIncrement:          utils.CheckTruth(tagSetting["AUTOINCREMENT"]),
		HasDefaultValue:        utils.CheckTruth(tagSetting["AUTOINCREMENT"]),
		NotNull:                utils.CheckTruth(tagSetting["NOT NULL"], tagSetting["NOTNULL"]),
		Unique:                 utils.CheckTruth(tagSetting["UNIQUE"]),
		Comment:                tagSetting["COMMENT"],
		AutoIncrementIncrement: 1,
	}

	for field.IndirectFieldType.Kind() == reflect.Ptr { // 如果字段是指针，会通过 Elem 拿到实际类型
		field.IndirectFieldType = field.IndirectFieldType.Elem()
	}

	fieldValue := reflect.New(field.IndirectFieldType) // 创建一个实际类型实例
	// if field is valuer, used its value or first field as data type
	valuer, isValuer := fieldValue.Interface().(driver.Valuer)
	if isValuer { // 如果实现了 driver.Valuer 接口
		if _, ok := fieldValue.Interface().(GormDataTypeInterface); !ok {
			if v, err := valuer.Value(); reflect.ValueOf(v).IsValid() && err == nil {
				fieldValue = reflect.ValueOf(v) // 如果没有实现 GormDataTypeInterface， 则当做 driver.Valuer 对待，调用 Value() 方法，获取 value
			}

			// Use the field struct's first field type as data type, e.g: use `string` for sql.NullString
			var getRealFieldValue func(reflect.Value)
			getRealFieldValue = func(v reflect.Value) {
				var (
					rv     = reflect.Indirect(v)
					rvType = rv.Type()
				)

				if rv.Kind() == reflect.Struct && !rvType.ConvertibleTo(TimeReflectType) { // 如果当前值是结构体，并且不能被转换为 time.Time
					for i := 0; i < rvType.NumField(); i++ {
						for key, value := range ParseTagSetting(rvType.Field(i).Tag.Get("gorm"), ";") {
							if _, ok := field.TagSettings[key]; !ok {
								field.TagSettings[key] = value // 解析结构体的所有字段的 gorm 注解，添加到 field.TagSettings 里面
							}
						}
					}

					for i := 0; i < rvType.NumField(); i++ {
						newFieldType := rvType.Field(i).Type
						for newFieldType.Kind() == reflect.Ptr {
							newFieldType = newFieldType.Elem()
						} // 如果该类型是指针，取出实际类型

						fieldValue = reflect.New(newFieldType)
						if rvType != reflect.Indirect(fieldValue).Type() {
							getRealFieldValue(fieldValue) // 递归解析
						}

						if fieldValue.IsValid() { // 遇到第一个解析成功的类型，作为该字段类型
							return
						}
					}
				}
			}

			getRealFieldValue(fieldValue)
		}
	}

	if v, isSerializer := fieldValue.Interface().(SerializerInterface); isSerializer {
		field.DataType = String // 如果实现了 SerializerInterface 接口，则将字段的数据类型设置为 String
		field.Serializer = v
	} else {
		serializerName := field.TagSettings["JSON"]
		if serializerName == "" {
			serializerName = field.TagSettings["SERIALIZER"]
		} // SERIALIZER 注解优先级比 JSON 注解高
		if serializerName != "" { // 如果配置了 JSON 或者 SERIALIZER 注解
			if serializer, ok := GetSerializer(serializerName); ok {
				// Set default data type to string for serializer
				field.DataType = String // 从全局注册的序列化器中根据名字找到对应的序列化器
				field.Serializer = serializer
			} else { // 找不到序列化器，报错
				schema.err = fmt.Errorf("invalid serializer type %v", serializerName)
			}
		}
	}

	if num, ok := field.TagSettings["AUTOINCREMENTINCREMENT"]; ok { // 设置了 AUTOINCREMENTINCREMENT 注解，指定了自增的起始值
		field.AutoIncrementIncrement, _ = strconv.ParseInt(num, 10, 64)
	}

	if v, ok := field.TagSettings["DEFAULT"]; ok {
		field.HasDefaultValue = true
		field.DefaultValue = v // 配置了 DEFAULT 注解，设置默认值
	}

	if num, ok := field.TagSettings["SIZE"]; ok {
		if field.Size, err = strconv.Atoi(num); err != nil {
			field.Size = -1 // 配置了 SIZE 注解，设置 Size
		}
	}

	if p, ok := field.TagSettings["PRECISION"]; ok {
		field.Precision, _ = strconv.Atoi(p) // 精度
	}

	if s, ok := field.TagSettings["SCALE"]; ok {
		field.Scale, _ = strconv.Atoi(s) // 小数位数的精度
	}

	// default value is function or null or blank (primary keys)
	field.DefaultValue = strings.TrimSpace(field.DefaultValue)
	// 如果默认值包含 ( ), 或者是 null, "" , 不解析默认值
	skipParseDefaultValue := strings.Contains(field.DefaultValue, "(") &&
		strings.Contains(field.DefaultValue, ")") || strings.ToLower(field.DefaultValue) == "null" || field.DefaultValue == ""
	switch reflect.Indirect(fieldValue).Kind() {
	case reflect.Bool:
		field.DataType = Bool
		if field.HasDefaultValue && !skipParseDefaultValue { // 解析默认值到 DefaultValueInterface
			if field.DefaultValueInterface, err = strconv.ParseBool(field.DefaultValue); err != nil {
				schema.err = fmt.Errorf("failed to parse %s as default value for bool, got error: %v", field.DefaultValue, err)
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		field.DataType = Int
		if field.HasDefaultValue && !skipParseDefaultValue {
			if field.DefaultValueInterface, err = strconv.ParseInt(field.DefaultValue, 0, 64); err != nil {
				schema.err = fmt.Errorf("failed to parse %s as default value for int, got error: %v", field.DefaultValue, err)
			}
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		field.DataType = Uint
		if field.HasDefaultValue && !skipParseDefaultValue {
			if field.DefaultValueInterface, err = strconv.ParseUint(field.DefaultValue, 0, 64); err != nil {
				schema.err = fmt.Errorf("failed to parse %s as default value for uint, got error: %v", field.DefaultValue, err)
			}
		}
	case reflect.Float32, reflect.Float64:
		field.DataType = Float
		if field.HasDefaultValue && !skipParseDefaultValue {
			if field.DefaultValueInterface, err = strconv.ParseFloat(field.DefaultValue, 64); err != nil {
				schema.err = fmt.Errorf("failed to parse %s as default value for float, got error: %v", field.DefaultValue, err)
			}
		}
	case reflect.String:
		field.DataType = String
		if field.HasDefaultValue && !skipParseDefaultValue {
			field.DefaultValue = strings.Trim(field.DefaultValue, "'")
			field.DefaultValue = strings.Trim(field.DefaultValue, `"`)
			field.DefaultValueInterface = field.DefaultValue
		}
	case reflect.Struct:
		if _, ok := fieldValue.Interface().(*time.Time); ok { // 各种形式的 time， 及其衍生类型
			field.DataType = Time
		} else if fieldValue.Type().ConvertibleTo(TimeReflectType) {
			field.DataType = Time
		} else if fieldValue.Type().ConvertibleTo(TimePtrReflectType) {
			field.DataType = Time
		}
		if field.HasDefaultValue && !skipParseDefaultValue && field.DataType == Time {
			if t, err := now.Parse(field.DefaultValue); err == nil {
				field.DefaultValueInterface = t
			}
		}
	case reflect.Array, reflect.Slice:
		if reflect.Indirect(fieldValue).Type().Elem() == ByteReflectType && field.DataType == "" {
			field.DataType = Bytes
		}
	}

	if dataTyper, ok := fieldValue.Interface().(GormDataTypeInterface); ok {
		field.DataType = DataType(dataTyper.GormDataType()) // 如果实现 GormDataTypeInterface ，可指定 DataType
	}

	// 以下情况会自动设置创建时间
	// 1. 带有 AUTOCREATETIME 注解，
	// 2. 属性名叫做：CreatedAt 并且类型在 (Time, Int, Uint) 里面
	if v, ok := field.TagSettings["AUTOCREATETIME"]; (ok && utils.CheckTruth(v)) || (!ok && field.Name == "CreatedAt" && (field.DataType == Time || field.DataType == Int || field.DataType == Uint)) {
		if field.DataType == Time {
			field.AutoCreateTime = UnixTime
		} else if strings.ToUpper(v) == "NANO" {
			field.AutoCreateTime = UnixNanosecond
		} else if strings.ToUpper(v) == "MILLI" {
			field.AutoCreateTime = UnixMillisecond
		} else {
			field.AutoCreateTime = UnixSecond
		}
	}

	// 以下情况之一会在创建和更新的时候自动设置更新时间
	// 1. 带有 AUTOUPDATETIME 注解
	// 2. 名字为 UpdatedAt，并且类型在 (Time, Int, Uint) 里面
	if v, ok := field.TagSettings["AUTOUPDATETIME"]; (ok && utils.CheckTruth(v)) || (!ok && field.Name == "UpdatedAt" && (field.DataType == Time || field.DataType == Int || field.DataType == Uint)) {
		if field.DataType == Time {
			field.AutoUpdateTime = UnixTime
		} else if strings.ToUpper(v) == "NANO" {
			field.AutoUpdateTime = UnixNanosecond
		} else if strings.ToUpper(v) == "MILLI" {
			field.AutoUpdateTime = UnixMillisecond
		} else {
			field.AutoUpdateTime = UnixSecond
		}
	}

	if field.GORMDataType == "" {
		field.GORMDataType = field.DataType
	}

	// 如果带了 TYPE 注解
	// 根据解析出来的 type 来设置 DataType
	if val, ok := field.TagSettings["TYPE"]; ok {
		switch DataType(strings.ToLower(val)) {
		case Bool, Int, Uint, Float, String, Time, Bytes:
			field.DataType = DataType(strings.ToLower(val))
		default:
			field.DataType = DataType(val)
		}
	}

	if field.Size == 0 { // Size 没有设置， 根据数据类型生成 size
		switch reflect.Indirect(fieldValue).Kind() {
		case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64, reflect.Float64:
			field.Size = 64
		case reflect.Int8, reflect.Uint8:
			field.Size = 8
		case reflect.Int16, reflect.Uint16:
			field.Size = 16
		case reflect.Int32, reflect.Uint32, reflect.Float32:
			field.Size = 32
		}
	}

	// setup permission
	if val, ok := field.TagSettings["-"]; ok {
		val = strings.ToLower(strings.TrimSpace(val))
		switch val {
		case "-": // 任何情况都忽略该字段
			field.Creatable = false
			field.Updatable = false
			field.Readable = false
			field.DataType = ""
		case "all": // 任何情况都忽略该字段
			field.Creatable = false
			field.Updatable = false
			field.Readable = false
			field.DataType = ""
			field.IgnoreMigration = true
		case "migration": // 只在 migration 时忽略该字段
			field.IgnoreMigration = true
		}
	}

	if v, ok := field.TagSettings["->"]; ok { // 不可写，读取看配置
		field.Creatable = false
		field.Updatable = false
		if strings.ToLower(v) == "false" {
			field.Readable = false
		} else {
			field.Readable = true
		}
	}

	if v, ok := field.TagSettings["<-"]; ok { // 配置先权限
		field.Creatable = true
		field.Updatable = true

		if v != "<-" {
			if !strings.Contains(v, "create") { // 不能创建
				field.Creatable = false
			}

			if !strings.Contains(v, "update") { // 不能更新
				field.Updatable = false
			}
		}
	}

	// Normal anonymous field or having `EMBEDDED` tag
	// 以下情况之一会当做 EMBEDDED model,
	// 1. 带有 EMBEDDED 注解
	// 2. 类型不为 (Time, Bytes), 并且没实现 driver.Valuer 接口，并且为嵌入字段，并且有(可创建，可更新，可读)权限之一)
	if _, ok := field.TagSettings["EMBEDDED"]; ok || (field.GORMDataType != Time && field.GORMDataType != Bytes && !isValuer &&
		fieldStruct.Anonymous && (field.Creatable || field.Updatable || field.Readable)) {
		kind := reflect.Indirect(fieldValue).Kind()
		switch kind {
		case reflect.Struct: // 如果是结构体，是嵌套结构
			var err error
			// 后续操作忽略该字段
			field.Creatable = false
			field.Updatable = false
			field.Readable = false

			cacheStore := &sync.Map{}
			cacheStore.Store(embeddedCacheKey, true)
			// 解析该嵌入类型的 schema
			if field.EmbeddedSchema, err = getOrParse(fieldValue.Interface(), cacheStore, embeddedNamer{Table: schema.Table, Namer: schema.namer}); err != nil {
				schema.err = err
			}

			for _, ef := range field.EmbeddedSchema.Fields {
				ef.Schema = schema
				ef.OwnerSchema = field.EmbeddedSchema
				ef.BindNames = append([]string{fieldStruct.Name}, ef.BindNames...)
				// index is negative means is pointer
				if field.FieldType.Kind() == reflect.Struct {
					ef.StructField.Index = append([]int{fieldStruct.Index[0]}, ef.StructField.Index...)
				} else { // 嵌套的是一个指针
					ef.StructField.Index = append([]int{-fieldStruct.Index[0] - 1}, ef.StructField.Index...)
				}

				if prefix, ok := field.TagSettings["EMBEDDEDPREFIX"]; ok && ef.DBName != "" {
					ef.DBName = prefix + ef.DBName // 如果定义了 EMBEDDEDPREFIX 注解，给 DBName 加一个前缀
				}

				if ef.PrimaryKey {
					// 嵌套结构体被解析为主键（可能是名字叫 ID）
					if !utils.CheckTruth(ef.TagSettings["PRIMARYKEY"], ef.TagSettings["PRIMARY_KEY"]) {
						// 只要不是显式有 PRIMARYKEY 注解，都不算注解
						ef.PrimaryKey = false

						// 没有显式定义 AUTOINCREMENT， 也不算自增
						if val, ok := ef.TagSettings["AUTOINCREMENT"]; !ok || !utils.CheckTruth(val) {
							ef.AutoIncrement = false
						}

						// 由于 AUTOINCREMENT 会被当做有默认值，如果自增被取消了，这里的 HasDefaultValue 也要被取消
						if !ef.AutoIncrement && ef.DefaultValue == "" {
							ef.HasDefaultValue = false
						}
					}
				}

				for k, v := range field.TagSettings {
					ef.TagSettings[k] = v // 嵌套结构体字段的 tag Setting 也会收集到嵌套结构体的 TagSetting 里面
				}
			}
		case reflect.Invalid, reflect.Uintptr, reflect.Array, reflect.Chan, reflect.Func, reflect.Interface,
			reflect.Map, reflect.Ptr, reflect.Slice, reflect.UnsafePointer, reflect.Complex64, reflect.Complex128:
			schema.err = fmt.Errorf("invalid embedded struct for %s's field %s, should be struct, but got %v", field.Schema.Name, field.Name, field.FieldType)
		}
	}

	return field
}

// create valuer, setter when parse struct
func (field *Field) setupValuerAndSetter() {
	// Setup NewValuePool
	field.setupNewValuePool()

	// ValueOf returns field's value and if it is zero
	fieldIndex := field.StructField.Index[0]
	switch {
	case len(field.StructField.Index) == 1 && fieldIndex > 0: // 非嵌套结构体场景
		field.ValueOf = func(ctx context.Context, value reflect.Value) (interface{}, bool) {
			fieldValue := reflect.Indirect(value).Field(fieldIndex)
			return fieldValue.Interface(), fieldValue.IsZero()
		}
	default: // 嵌套结构体
		field.ValueOf = func(ctx context.Context, v reflect.Value) (interface{}, bool) {
			v = reflect.Indirect(v)
			// 嵌套结构体的 v 倒序存在 Index 里面
			for _, fieldIdx := range field.StructField.Index {
				// 该字段是嵌套的， 传进来的 v 是最外层 model 结构体，Index 就是每一层对应的下标
				// 如果上一层是结构体
				if fieldIdx >= 0 { // 字段是一个结构体
					v = v.Field(fieldIdx)
				} else { // 如果上一层是一个指针
					v = v.Field(-fieldIdx - 1)

					if !v.IsNil() {
						v = v.Elem()
					} else {
						return nil, true
					}
				}
			}

			fv, zero := v.Interface(), v.IsZero()
			return fv, zero
		}
	}

	if field.Serializer != nil {
		oldValuerOf := field.ValueOf
		field.ValueOf = func(ctx context.Context, v reflect.Value) (interface{}, bool) {
			value, zero := oldValuerOf(ctx, v)

			s, ok := value.(SerializerValuerInterface)
			if !ok {
				s = field.Serializer
			}

			return &serializer{
				Field:           field,
				SerializeValuer: s,
				Destination:     v,
				Context:         ctx,
				fieldValue:      value,
			}, zero
		}
	}

	// ReflectValueOf returns field's reflect value
	switch {
	case len(field.StructField.Index) == 1 && fieldIndex > 0:
		field.ReflectValueOf = func(ctx context.Context, value reflect.Value) reflect.Value {
			return reflect.Indirect(value).Field(fieldIndex)
		}
	default:
		field.ReflectValueOf = func(ctx context.Context, v reflect.Value) reflect.Value {
			v = reflect.Indirect(v)
			for idx, fieldIdx := range field.StructField.Index {
				if fieldIdx >= 0 {
					v = v.Field(fieldIdx)
				} else {
					v = v.Field(-fieldIdx - 1)

					if v.IsNil() {
						v.Set(reflect.New(v.Type().Elem()))
					}

					if idx < len(field.StructField.Index)-1 {
						v = v.Elem()
					}
				}
			}
			return v
		}
	}

	fallbackSetter := func(ctx context.Context, value reflect.Value, v interface{}, setter func(context.Context, reflect.Value, interface{}) error) (err error) {
		if v == nil {
			field.ReflectValueOf(ctx, value).Set(reflect.New(field.FieldType).Elem())
		} else {
			reflectV := reflect.ValueOf(v)
			// Optimal value type acquisition for v
			reflectValType := reflectV.Type()

			if reflectValType.AssignableTo(field.FieldType) {
				if reflectV.Kind() == reflect.Ptr && reflectV.Elem().Kind() == reflect.Ptr {
					reflectV = reflect.Indirect(reflectV)
				}
				field.ReflectValueOf(ctx, value).Set(reflectV)
				return
			} else if reflectValType.ConvertibleTo(field.FieldType) {
				field.ReflectValueOf(ctx, value).Set(reflectV.Convert(field.FieldType))
				return
			} else if field.FieldType.Kind() == reflect.Ptr {
				fieldValue := field.ReflectValueOf(ctx, value)
				fieldType := field.FieldType.Elem()

				if reflectValType.AssignableTo(fieldType) {
					if !fieldValue.IsValid() {
						fieldValue = reflect.New(fieldType)
					} else if fieldValue.IsNil() {
						fieldValue.Set(reflect.New(fieldType))
					}
					fieldValue.Elem().Set(reflectV)
					return
				} else if reflectValType.ConvertibleTo(fieldType) {
					if fieldValue.IsNil() {
						fieldValue.Set(reflect.New(fieldType))
					}

					fieldValue.Elem().Set(reflectV.Convert(fieldType))
					return
				}
			}

			if reflectV.Kind() == reflect.Ptr {
				if reflectV.IsNil() {
					field.ReflectValueOf(ctx, value).Set(reflect.New(field.FieldType).Elem())
				} else if reflectV.Type().Elem().AssignableTo(field.FieldType) {
					field.ReflectValueOf(ctx, value).Set(reflectV.Elem())
					return
				} else {
					err = setter(ctx, value, reflectV.Elem().Interface())
				}
			} else if valuer, ok := v.(driver.Valuer); ok {
				if v, err = valuer.Value(); err == nil {
					err = setter(ctx, value, v)
				}
			} else if _, ok := v.(clause.Expr); !ok {
				return fmt.Errorf("failed to set value %#v to field %s", v, field.Name)
			}
		}

		return
	}

	// Set
	switch field.FieldType.Kind() {
	case reflect.Bool:
		field.Set = func(ctx context.Context, value reflect.Value, v interface{}) error {
			switch data := v.(type) {
			case **bool:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetBool(**data)
				}
			case bool:
				field.ReflectValueOf(ctx, value).SetBool(data)
			case int64:
				field.ReflectValueOf(ctx, value).SetBool(data > 0)
			case string:
				b, _ := strconv.ParseBool(data)
				field.ReflectValueOf(ctx, value).SetBool(b)
			default:
				return fallbackSetter(ctx, value, v, field.Set)
			}
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		field.Set = func(ctx context.Context, value reflect.Value, v interface{}) (err error) {
			switch data := v.(type) {
			case **int64:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetInt(**data)
				}
			case **int:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetInt(int64(**data))
				}
			case **int8:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetInt(int64(**data))
				}
			case **int16:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetInt(int64(**data))
				}
			case **int32:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetInt(int64(**data))
				}
			case int64:
				field.ReflectValueOf(ctx, value).SetInt(data)
			case int:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case int8:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case int16:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case int32:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case uint:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case uint8:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case uint16:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case uint32:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case uint64:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case float32:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case float64:
				field.ReflectValueOf(ctx, value).SetInt(int64(data))
			case []byte:
				return field.Set(ctx, value, string(data))
			case string:
				if i, err := strconv.ParseInt(data, 0, 64); err == nil {
					field.ReflectValueOf(ctx, value).SetInt(i)
				} else {
					return err
				}
			case time.Time:
				if field.AutoCreateTime == UnixNanosecond || field.AutoUpdateTime == UnixNanosecond {
					field.ReflectValueOf(ctx, value).SetInt(data.UnixNano())
				} else if field.AutoCreateTime == UnixMillisecond || field.AutoUpdateTime == UnixMillisecond {
					field.ReflectValueOf(ctx, value).SetInt(data.UnixNano() / 1e6)
				} else {
					field.ReflectValueOf(ctx, value).SetInt(data.Unix())
				}
			case *time.Time:
				if data != nil {
					if field.AutoCreateTime == UnixNanosecond || field.AutoUpdateTime == UnixNanosecond {
						field.ReflectValueOf(ctx, value).SetInt(data.UnixNano())
					} else if field.AutoCreateTime == UnixMillisecond || field.AutoUpdateTime == UnixMillisecond {
						field.ReflectValueOf(ctx, value).SetInt(data.UnixNano() / 1e6)
					} else {
						field.ReflectValueOf(ctx, value).SetInt(data.Unix())
					}
				} else {
					field.ReflectValueOf(ctx, value).SetInt(0)
				}
			default:
				return fallbackSetter(ctx, value, v, field.Set)
			}
			return err
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		field.Set = func(ctx context.Context, value reflect.Value, v interface{}) (err error) {
			switch data := v.(type) {
			case **uint64:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetUint(**data)
				}
			case **uint:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetUint(uint64(**data))
				}
			case **uint8:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetUint(uint64(**data))
				}
			case **uint16:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetUint(uint64(**data))
				}
			case **uint32:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetUint(uint64(**data))
				}
			case uint64:
				field.ReflectValueOf(ctx, value).SetUint(data)
			case uint:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case uint8:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case uint16:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case uint32:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case int64:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case int:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case int8:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case int16:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case int32:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case float32:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case float64:
				field.ReflectValueOf(ctx, value).SetUint(uint64(data))
			case []byte:
				return field.Set(ctx, value, string(data))
			case time.Time:
				if field.AutoCreateTime == UnixNanosecond || field.AutoUpdateTime == UnixNanosecond {
					field.ReflectValueOf(ctx, value).SetUint(uint64(data.UnixNano()))
				} else if field.AutoCreateTime == UnixMillisecond || field.AutoUpdateTime == UnixMillisecond {
					field.ReflectValueOf(ctx, value).SetUint(uint64(data.UnixNano() / 1e6))
				} else {
					field.ReflectValueOf(ctx, value).SetUint(uint64(data.Unix()))
				}
			case string:
				if i, err := strconv.ParseUint(data, 0, 64); err == nil {
					field.ReflectValueOf(ctx, value).SetUint(i)
				} else {
					return err
				}
			default:
				return fallbackSetter(ctx, value, v, field.Set)
			}
			return err
		}
	case reflect.Float32, reflect.Float64:
		field.Set = func(ctx context.Context, value reflect.Value, v interface{}) (err error) {
			switch data := v.(type) {
			case **float64:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetFloat(**data)
				}
			case **float32:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetFloat(float64(**data))
				}
			case float64:
				field.ReflectValueOf(ctx, value).SetFloat(data)
			case float32:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case int64:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case int:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case int8:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case int16:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case int32:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case uint:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case uint8:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case uint16:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case uint32:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case uint64:
				field.ReflectValueOf(ctx, value).SetFloat(float64(data))
			case []byte:
				return field.Set(ctx, value, string(data))
			case string:
				if i, err := strconv.ParseFloat(data, 64); err == nil {
					field.ReflectValueOf(ctx, value).SetFloat(i)
				} else {
					return err
				}
			default:
				return fallbackSetter(ctx, value, v, field.Set)
			}
			return err
		}
	case reflect.String:
		field.Set = func(ctx context.Context, value reflect.Value, v interface{}) (err error) {
			switch data := v.(type) {
			case **string:
				if data != nil && *data != nil {
					field.ReflectValueOf(ctx, value).SetString(**data)
				}
			case string:
				field.ReflectValueOf(ctx, value).SetString(data)
			case []byte:
				field.ReflectValueOf(ctx, value).SetString(string(data))
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				field.ReflectValueOf(ctx, value).SetString(utils.ToString(data))
			case float64, float32:
				field.ReflectValueOf(ctx, value).SetString(fmt.Sprintf("%."+strconv.Itoa(field.Precision)+"f", data))
			default:
				return fallbackSetter(ctx, value, v, field.Set)
			}
			return err
		}
	default:
		fieldValue := reflect.New(field.FieldType)
		switch fieldValue.Elem().Interface().(type) {
		case time.Time:
			field.Set = func(ctx context.Context, value reflect.Value, v interface{}) error {
				switch data := v.(type) {
				case **time.Time:
					if data != nil && *data != nil {
						field.Set(ctx, value, *data)
					}
				case time.Time:
					field.ReflectValueOf(ctx, value).Set(reflect.ValueOf(v))
				case *time.Time:
					if data != nil {
						field.ReflectValueOf(ctx, value).Set(reflect.ValueOf(data).Elem())
					} else {
						field.ReflectValueOf(ctx, value).Set(reflect.ValueOf(time.Time{}))
					}
				case string:
					if t, err := now.Parse(data); err == nil {
						field.ReflectValueOf(ctx, value).Set(reflect.ValueOf(t))
					} else {
						return fmt.Errorf("failed to set string %v to time.Time field %s, failed to parse it as time, got error %v", v, field.Name, err)
					}
				default:
					return fallbackSetter(ctx, value, v, field.Set)
				}
				return nil
			}
		case *time.Time:
			field.Set = func(ctx context.Context, value reflect.Value, v interface{}) error {
				switch data := v.(type) {
				case **time.Time:
					if data != nil {
						field.ReflectValueOf(ctx, value).Set(reflect.ValueOf(*data))
					}
				case time.Time:
					fieldValue := field.ReflectValueOf(ctx, value)
					if fieldValue.IsNil() {
						fieldValue.Set(reflect.New(field.FieldType.Elem()))
					}
					fieldValue.Elem().Set(reflect.ValueOf(v))
				case *time.Time:
					field.ReflectValueOf(ctx, value).Set(reflect.ValueOf(v))
				case string:
					if t, err := now.Parse(data); err == nil {
						fieldValue := field.ReflectValueOf(ctx, value)
						if fieldValue.IsNil() {
							if v == "" {
								return nil
							}
							fieldValue.Set(reflect.New(field.FieldType.Elem()))
						}
						fieldValue.Elem().Set(reflect.ValueOf(t))
					} else {
						return fmt.Errorf("failed to set string %v to time.Time field %s, failed to parse it as time, got error %v", v, field.Name, err)
					}
				default:
					return fallbackSetter(ctx, value, v, field.Set)
				}
				return nil
			}
		default:
			if _, ok := fieldValue.Elem().Interface().(sql.Scanner); ok {
				// pointer scanner
				field.Set = func(ctx context.Context, value reflect.Value, v interface{}) (err error) {
					reflectV := reflect.ValueOf(v)
					if !reflectV.IsValid() {
						field.ReflectValueOf(ctx, value).Set(reflect.New(field.FieldType).Elem())
					} else if reflectV.Type().AssignableTo(field.FieldType) {
						field.ReflectValueOf(ctx, value).Set(reflectV)
					} else if reflectV.Kind() == reflect.Ptr {
						if reflectV.IsNil() || !reflectV.IsValid() {
							field.ReflectValueOf(ctx, value).Set(reflect.New(field.FieldType).Elem())
						} else {
							return field.Set(ctx, value, reflectV.Elem().Interface())
						}
					} else {
						fieldValue := field.ReflectValueOf(ctx, value)
						if fieldValue.IsNil() {
							fieldValue.Set(reflect.New(field.FieldType.Elem()))
						}

						if valuer, ok := v.(driver.Valuer); ok {
							v, _ = valuer.Value()
						}

						err = fieldValue.Interface().(sql.Scanner).Scan(v)
					}
					return
				}
			} else if _, ok := fieldValue.Interface().(sql.Scanner); ok {
				// struct scanner
				field.Set = func(ctx context.Context, value reflect.Value, v interface{}) (err error) {
					reflectV := reflect.ValueOf(v)
					if !reflectV.IsValid() {
						field.ReflectValueOf(ctx, value).Set(reflect.New(field.FieldType).Elem())
					} else if reflectV.Type().AssignableTo(field.FieldType) {
						field.ReflectValueOf(ctx, value).Set(reflectV)
					} else if reflectV.Kind() == reflect.Ptr {
						if reflectV.IsNil() || !reflectV.IsValid() {
							field.ReflectValueOf(ctx, value).Set(reflect.New(field.FieldType).Elem())
						} else {
							return field.Set(ctx, value, reflectV.Elem().Interface())
						}
					} else {
						if valuer, ok := v.(driver.Valuer); ok {
							v, _ = valuer.Value()
						}

						err = field.ReflectValueOf(ctx, value).Addr().Interface().(sql.Scanner).Scan(v)
					}
					return
				}
			} else {
				field.Set = func(ctx context.Context, value reflect.Value, v interface{}) (err error) {
					return fallbackSetter(ctx, value, v, field.Set)
				}
			}
		}
	}

	if field.Serializer != nil {
		var (
			oldFieldSetter = field.Set
			sameElemType   bool
			sameType       = field.FieldType == reflect.ValueOf(field.Serializer).Type()
		)

		if reflect.ValueOf(field.Serializer).Kind() == reflect.Ptr {
			sameElemType = field.FieldType == reflect.ValueOf(field.Serializer).Type().Elem()
		}

		serializerValue := reflect.Indirect(reflect.ValueOf(field.Serializer))
		serializerType := serializerValue.Type()
		field.Set = func(ctx context.Context, value reflect.Value, v interface{}) (err error) {
			if s, ok := v.(*serializer); ok {
				if s.fieldValue != nil {
					err = oldFieldSetter(ctx, value, s.fieldValue)
				} else if err = s.Serializer.Scan(ctx, field, value, s.value); err == nil {
					if sameElemType {
						field.ReflectValueOf(ctx, value).Set(reflect.ValueOf(s.Serializer).Elem())
					} else if sameType {
						field.ReflectValueOf(ctx, value).Set(reflect.ValueOf(s.Serializer))
					}
					si := reflect.New(serializerType)
					si.Elem().Set(serializerValue)
					s.Serializer = si.Interface().(SerializerInterface)
				}
			} else {
				err = oldFieldSetter(ctx, value, v)
			}
			return
		}
	}
}

func (field *Field) setupNewValuePool() {
	if field.Serializer != nil {
		serializerValue := reflect.Indirect(reflect.ValueOf(field.Serializer))
		serializerType := serializerValue.Type()
		field.NewValuePool = &sync.Pool{
			New: func() interface{} {
				si := reflect.New(serializerType)
				si.Elem().Set(serializerValue)
				return &serializer{
					Field:      field,
					Serializer: si.Interface().(SerializerInterface),
				}
			},
		}
	}

	if field.NewValuePool == nil {
		field.NewValuePool = poolInitializer(reflect.PtrTo(field.IndirectFieldType))
	}
}
