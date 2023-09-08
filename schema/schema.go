package schema

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"reflect"
	"strings"
	"sync"

	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// ErrUnsupportedDataType unsupported data type
var ErrUnsupportedDataType = errors.New("unsupported data type")

type Schema struct {
	Name                    string            // model 结构体的 Name
	ModelType               reflect.Type      // model 结构体的类型
	Table                   string            // 该 schema 结构体对应的 db 的表名
	PrioritizedPrimaryField *Field            // 优先选择的主键字段 Field 定义
	DBNames                 []string          // 当前 model 包含的所有 db COLUMN 名
	PrimaryFields           []*Field          // 主键字段定义列表，多个就是复合主键
	PrimaryFieldDBNames     []string          // 优先选择的主键字段 db COLUMN 列表
	Fields                  []*Field          // 包含的每一个属性的定义，嵌套属性会展开
	FieldsByName            map[string]*Field // 结构体名字到 Field 的映射
	FieldsByBindName        map[string]*Field // embedded fields is 'Embed.Field' 带嵌套结构体 path 的字段到 Filed 的映射
	FieldsByDBName          map[string]*Field // db COLUMN 名到 Field 的映射
	// 有默认值，但是 默认值包含函数 ( ), 或者是 null, ""
	FieldsWithDefaultDBValue []*Field // fields with default value assigned by database
	// 保存表之间的关联关系
	Relationships Relationships
	// 可以在 Create Query Update Delete 的时候修改 sql 定义， model 实现 CreateClausesInterface 等接口
	// 如 DeleteAt 类型， 可以在删除的时候将 deleteAt 设置成当前时间
	CreateClauses []clause.Interface
	QueryClauses  []clause.Interface
	UpdateClauses []clause.Interface
	DeleteClauses []clause.Interface

	// 是否带有对应的回调方法
	// schema/schema.go:308 通过反射赋值的
	BeforeCreate, AfterCreate bool
	BeforeUpdate, AfterUpdate bool
	BeforeDelete, AfterDelete bool
	BeforeSave, AfterSave     bool
	AfterFind                 bool

	// 解析 scheme 的时候如果报错了，存储在这里，其他正在等待初始化的协程也会读取到错误
	err error
	// 是否解析完成的 channel, 初始化完成，就关闭 channel
	initialized chan struct{}
	// 包含名称转换的策略
	namer Namer
	// 缓存一些参数或者结构体 scheme
	cacheStore *sync.Map
}

func (schema Schema) String() string {
	if schema.ModelType.Name() == "" {
		return fmt.Sprintf("%s(%s)", schema.Name, schema.Table)
	}
	return fmt.Sprintf("%s.%s", schema.ModelType.PkgPath(), schema.ModelType.Name())
}

func (schema Schema) MakeSlice() reflect.Value {
	slice := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(schema.ModelType)), 0, 20)
	results := reflect.New(slice.Type())
	results.Elem().Set(slice)
	return results
}

// LookUpField 通过结构体名字或者 db COLUMN 找到字段的 Field
func (schema Schema) LookUpField(name string) *Field {
	if field, ok := schema.FieldsByDBName[name]; ok {
		return field
	}
	if field, ok := schema.FieldsByName[name]; ok {
		return field
	}
	return nil
}

// LookUpFieldByBindName looks for the closest field in the embedded struct.
//
//	type Struct struct {
//		Embedded struct {
//			ID string // is selected by LookUpFieldByBindName([]string{"Embedded", "ID"}, "ID")
//		}
//		ID string // is selected by LookUpFieldByBindName([]string{"ID"}, "ID")
//	}
func (schema Schema) LookUpFieldByBindName(bindNames []string, name string) *Field {
	if len(bindNames) == 0 {
		return nil
	}
	for i := len(bindNames) - 1; i >= 0; i-- {
		find := strings.Join(bindNames[:i], ".") + "." + name
		if field, ok := schema.FieldsByBindName[find]; ok {
			return field
		}
	}
	return nil
}

type Tabler interface {
	TableName() string
}

type TablerWithNamer interface {
	TableName(Namer) string
}

// Parse get data type from dialector
func Parse(dest interface{}, cacheStore *sync.Map, namer Namer) (*Schema, error) {
	return ParseWithSpecialTableName(dest, cacheStore, namer, "")
}

// ParseWithSpecialTableName get data type from dialector with extra schema table
func ParseWithSpecialTableName(dest interface{}, cacheStore *sync.Map, namer Namer, specialTableName string) (*Schema, error) {
	if dest == nil {
		return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
	}

	value := reflect.ValueOf(dest)
	if value.Kind() == reflect.Ptr && value.IsNil() {
		value = reflect.New(value.Type().Elem()) // 如果是类型非空，但是指为空的指针，new 一个实例
	}
	modelType := reflect.Indirect(value).Type() // 如果 dest 的 type 是指针，取出实际的类型

	if modelType.Kind() == reflect.Interface { // 如果 dest 是一个接口，取出接口的实际类型
		modelType = reflect.Indirect(reflect.ValueOf(dest)).Elem().Type()
	}

	for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem() // 如果是 slice 或者 array， 或者指针, 取出实际的类型，可以取多层
	}

	if modelType.Kind() != reflect.Struct { // 经过上面的处理，这里 modelType 一定是一个结构体了
		if modelType.PkgPath() == "" {
			return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
		}
		return nil, fmt.Errorf("%w: %s.%s", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
	}

	// Cache the Schema for performance,
	// Use the modelType or modelType + schemaTable (if it present) as cache key.
	var schemaCacheKey interface{}
	if specialTableName != "" { // 生成 model 缓存的 key,
		schemaCacheKey = fmt.Sprintf("%p-%s", modelType, specialTableName) // 如果指定了别名，使用 type+别名作为 key
	} else {
		schemaCacheKey = modelType // 如果没指定别名，直接使用 modelType 作为 key
	}

	// Load exist schema cache, return if exists
	if v, ok := cacheStore.Load(schemaCacheKey); ok { // 如果找到缓存，就直接用缓存
		s := v.(*Schema)
		// Wait for the initialization of other goroutines to complete
		<-s.initialized // 缓存里面的 Schema 可能没初始化，需要等待初始化完成或者失败
		return s, s.err
	}

	modelValue := reflect.New(modelType)           // 根据结构体的 type, New 一个 结构体
	tableName := namer.TableName(modelType.Name()) // 调用 namer.TableName 生成一个表名
	if tabler, ok := modelValue.Interface().(Tabler); ok {
		tableName = tabler.TableName() // 如果 model 结构体实现了 Tabler 接口，优先使用 TableName 方法指定的名字
	}
	if tabler, ok := modelValue.Interface().(TablerWithNamer); ok {
		tableName = tabler.TableName(namer) // 如果 model 结构体实现了 TablerWithNamer 接口，优先使用 TableName 方法指定的名字
	}
	if en, ok := namer.(embeddedNamer); ok {
		tableName = en.Table // 如果这个结构体是一个嵌套结构体，使用所在结构体的 tableName
	}
	if specialTableName != "" && specialTableName != tableName {
		tableName = specialTableName // 如果指定了 specialTableName，优先用指定的 specialTableName 作为 tableName
	}

	schema := &Schema{
		Name:             modelType.Name(),
		ModelType:        modelType,
		Table:            tableName,
		FieldsByName:     map[string]*Field{},
		FieldsByBindName: map[string]*Field{},
		FieldsByDBName:   map[string]*Field{},
		Relationships:    Relationships{Relations: map[string]*Relationship{}},
		cacheStore:       cacheStore,
		namer:            namer,
		initialized:      make(chan struct{}),
	}
	// When the schema initialization is completed, the channel will be closed
	defer close(schema.initialized) // 初始化完成，就关闭 channel

	// Load exist schema cache, return if exists
	if v, ok := cacheStore.Load(schemaCacheKey); ok { // 再次检查，如果已经在缓存里面存在了，就等待初始化完成，然后返还结果
		s := v.(*Schema)
		// Wait for the initialization of other goroutines to complete
		<-s.initialized
		return s, s.err
	}

	for i := 0; i < modelType.NumField(); i++ {
		if fieldStruct := modelType.Field(i); ast.IsExported(fieldStruct.Name) { // 解析每一个导出的字段
			if field := schema.ParseField(fieldStruct); field.EmbeddedSchema != nil {
				schema.Fields = append(schema.Fields, field.EmbeddedSchema.Fields...) // 如果有嵌套结构体字段，将其所有字段的 schema 合并到当前结构体
			} else {
				schema.Fields = append(schema.Fields, field) // 如果不是嵌套结构体，添加到 Fileds
			}
		}
	}

	for _, field := range schema.Fields {
		// 如果没有定义 COLUMN 注解，通过 namer.ColumnName 自动生成
		if field.DBName == "" && field.DataType != "" {
			field.DBName = namer.ColumnName(schema.Table, field.Name)
		}

		bindName := field.BindName()
		if field.DBName != "" {
			// nonexistence or shortest path or first appear prioritized if has permission
			// - 如果当前字段还没添加到 FieldsByDBName，添加一下
			// - 或者是有权限，并且嵌套层数比之前添加的浅，替换之前的
			if v, ok := schema.FieldsByDBName[field.DBName]; !ok || ((field.Creatable || field.Updatable || field.Readable) && len(field.BindNames) < len(v.BindNames)) {
				// 如果是添加的case, 添加一下 dbNames
				if _, ok := schema.FieldsByDBName[field.DBName]; !ok {
					schema.DBNames = append(schema.DBNames, field.DBName)
				}
				// db COLUMN 名到 Field 的映射
				schema.FieldsByDBName[field.DBName] = field
				// 结构体名字到 Field 的映射
				schema.FieldsByName[field.Name] = field
				// 带嵌套结构体 path 的字段到 Filed 的映射
				schema.FieldsByBindName[bindName] = field

				if v != nil && v.PrimaryKey { // 如果之前添加的字段是主键
					for idx, f := range schema.PrimaryFields {
						if f == v {
							// 遍历主键 PrimaryFields 删除之前添加的字段
							schema.PrimaryFields = append(schema.PrimaryFields[0:idx], schema.PrimaryFields[idx+1:]...)
						}
					}
				}

				if field.PrimaryKey { // 如果当前字段是主键
					schema.PrimaryFields = append(schema.PrimaryFields, field) // 添加到 PrimaryFields
				}
			}
		}

		if of, ok := schema.FieldsByName[field.Name]; !ok || of.TagSettings["-"] == "-" {
			schema.FieldsByName[field.Name] = field
		}
		if of, ok := schema.FieldsByBindName[bindName]; !ok || of.TagSettings["-"] == "-" {
			schema.FieldsByBindName[bindName] = field
		}

		field.setupValuerAndSetter()
	}

	// 如果有 db COLUMN 或者 结构体 名字叫 id 或者 ID 的字段，优先将其当做主键
	prioritizedPrimaryField := schema.LookUpField("id")
	if prioritizedPrimaryField == nil {
		prioritizedPrimaryField = schema.LookUpField("ID")
	}

	if prioritizedPrimaryField != nil { // 存在优先主键字段的情况
		if prioritizedPrimaryField.PrimaryKey { // 如果这个字段本身已经解析出来是主键了
			schema.PrioritizedPrimaryField = prioritizedPrimaryField // 直接指定 schema.PrioritizedPrimaryField 的优先主键
		} else if len(schema.PrimaryFields) == 0 { // 如果这个字段还不是主键，将其定义改成主键，并且加到 PrimaryFields 里面
			prioritizedPrimaryField.PrimaryKey = true
			schema.PrioritizedPrimaryField = prioritizedPrimaryField
			schema.PrimaryFields = append(schema.PrimaryFields, prioritizedPrimaryField)
		}
	}

	if schema.PrioritizedPrimaryField == nil { // 如果没找到可能的 id 字段
		if len(schema.PrimaryFields) == 1 { // 如果可选主键字段里面只有 1 个
			schema.PrioritizedPrimaryField = schema.PrimaryFields[0] // 选择唯一的这个值作为优先选择主键
		} else if len(schema.PrimaryFields) > 1 { // 如果有多个可选的
			// If there are multiple primary keys, the AUTOINCREMENT field is prioritized
			for _, field := range schema.PrimaryFields {
				if field.AutoIncrement { // 带 AutoIncrement 的优先
					schema.PrioritizedPrimaryField = field
					break
				}
			}
		}
	}

	for _, field := range schema.PrimaryFields {
		schema.PrimaryFieldDBNames = append(schema.PrimaryFieldDBNames, field.DBName)
	}

	for _, field := range schema.Fields {
		// 如果字段有解析出 DataType， 并且有默认值，但是 默认值包含函数 ( ), 或者是 null, ""，或者是自增主键
		if field.DataType != "" && field.HasDefaultValue && field.DefaultValueInterface == nil {
			schema.FieldsWithDefaultDBValue = append(schema.FieldsWithDefaultDBValue, field)
		}
	}

	if field := schema.PrioritizedPrimaryField; field != nil { // 如果有优先主键值
		switch field.GORMDataType {
		case Int, Uint: // 并且类型是 int uint
			if _, ok := field.TagSettings["AUTOINCREMENT"]; !ok { // 并且不包含 AUTOINCREMENT 注解
				if !field.HasDefaultValue || field.DefaultValueInterface != nil {
					// 如果没有默认值，或者默认值包含函数 ( ), 或者是 null, ""
					// 将主键也添加到 FieldsWithDefaultDBValue， 由数据库分配默认值
					schema.FieldsWithDefaultDBValue = append(schema.FieldsWithDefaultDBValue, field)
				}
				// 主键默认自增，有默认值
				field.HasDefaultValue = true
				field.AutoIncrement = true
			}
		}
	}

	callbacks := []string{"BeforeCreate", "AfterCreate", "BeforeUpdate", "AfterUpdate", "BeforeSave", "AfterSave", "BeforeDelete", "AfterDelete", "AfterFind"}
	for _, name := range callbacks {
		if methodValue := modelValue.MethodByName(name); methodValue.IsValid() {
			// 如果 model 结构体定义了对应的 hook 方法
			switch methodValue.Type().String() {
			case "func(*gorm.DB) error": // TODO hack
				reflect.Indirect(reflect.ValueOf(schema)).FieldByName(name).SetBool(true)
			default:
				logger.Default.Warn(context.Background(), "Model %v don't match %vInterface, should be `%v(*gorm.DB) error`. Please see https://gorm.io/docs/hooks.html", schema, name, name)
			}
		}
	}

	// Cache the schema
	if v, loaded := cacheStore.LoadOrStore(schemaCacheKey, schema); loaded {
		s := v.(*Schema) // 尝试缓存，如果是已经有了，等待其初始化完成
		// Wait for the initialization of other goroutines to complete
		<-s.initialized
		return s, s.err
	}

	defer func() {
		if schema.err != nil {
			logger.Default.Error(context.Background(), schema.err.Error())
			cacheStore.Delete(modelType) // 如果初始化失败，删除缓存
		}
	}()

	if _, embedded := schema.cacheStore.Load(embeddedCacheKey); !embedded {
		// 如果当前的 schema 不是嵌套结构体的
		for _, field := range schema.Fields {
			if field.DataType == "" && (field.Creatable || field.Updatable || field.Readable) {
				// 如果 DataType 为空，解析关联关系
				if schema.parseRelation(field); schema.err != nil {
					return schema, schema.err
				} else {
					// 解析成功的话添加到 FieldsByName FieldsByBindName
					schema.FieldsByName[field.Name] = field
					schema.FieldsByBindName[field.BindName()] = field
				}
			}

			// 解析 ClausesInterface
			// 如果使用实现了这些接口的字段，
			// 可以在 Create Query Update Delete 的时候修改 sql 定义
			// 如 DeleteAt 类型， 可以在删除的时候将 deleteAt 设置成当前时间
			fieldValue := reflect.New(field.IndirectFieldType)
			fieldInterface := fieldValue.Interface()
			if fc, ok := fieldInterface.(CreateClausesInterface); ok {
				field.Schema.CreateClauses = append(field.Schema.CreateClauses, fc.CreateClauses(field)...)
			}

			if fc, ok := fieldInterface.(QueryClausesInterface); ok {
				field.Schema.QueryClauses = append(field.Schema.QueryClauses, fc.QueryClauses(field)...)
			}

			if fc, ok := fieldInterface.(UpdateClausesInterface); ok {
				field.Schema.UpdateClauses = append(field.Schema.UpdateClauses, fc.UpdateClauses(field)...)
			}

			if fc, ok := fieldInterface.(DeleteClausesInterface); ok {
				field.Schema.DeleteClauses = append(field.Schema.DeleteClauses, fc.DeleteClauses(field)...)
			}
		}
	}

	return schema, schema.err
}

func getOrParse(dest interface{}, cacheStore *sync.Map, namer Namer) (*Schema, error) {
	modelType := reflect.ValueOf(dest).Type()
	for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		if modelType.PkgPath() == "" {
			return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
		}
		return nil, fmt.Errorf("%w: %s.%s", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
	}

	if v, ok := cacheStore.Load(modelType); ok {
		return v.(*Schema), nil
	}

	return Parse(dest, cacheStore, namer)
}
