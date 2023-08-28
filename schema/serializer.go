package schema

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

var serializerMap = sync.Map{}

// RegisterSerializer register serializer
func RegisterSerializer(name string, serializer SerializerInterface) {
	serializerMap.Store(strings.ToLower(name), serializer)
}

// GetSerializer get serializer
func GetSerializer(name string) (serializer SerializerInterface, ok bool) {
	v, ok := serializerMap.Load(strings.ToLower(name))
	if ok {
		serializer, ok = v.(SerializerInterface)
	}
	return serializer, ok
}

func init() {
	RegisterSerializer("json", JSONSerializer{})
	RegisterSerializer("unixtime", UnixSecondSerializer{})
	RegisterSerializer("gob", GobSerializer{})
}

// Serializer field value serializer
type serializer struct {
	// 字段的 *Field 定义
	Field *Field
	// new 出来的一个序列化器实例，用来从 db value 反序列化，一些反序列化器就是值本身
	Serializer SerializerInterface
	// 用来序列化属性，调用其 value 方法， 转换为 driver.Value
	SerializeValuer SerializerValuerInterface
	// model 结构体
	Destination reflect.Value
	Context     context.Context
	// db 里面取出来的值
	value interface{}
	// 结构体对应字段存的值
	fieldValue interface{}
}

// Scan implements sql.Scanner interface
// 读取 db value
func (s *serializer) Scan(value interface{}) error {
	s.value = value
	return nil
}

// Value implements driver.Valuer interface
// 从 model 的字段值转换为 driver.Value
func (s serializer) Value() (driver.Value, error) {
	return s.SerializeValuer.Value(s.Context, s.Field, s.Destination, s.fieldValue)
}

// SerializerInterface serializer interface
type SerializerInterface interface {
	Scan(ctx context.Context, field *Field, dst reflect.Value, dbValue interface{}) error
	SerializerValuerInterface
}

// SerializerValuerInterface serializer valuer interface
type SerializerValuerInterface interface {
	Value(ctx context.Context, field *Field, dst reflect.Value, fieldValue interface{}) (interface{}, error)
}

// JSONSerializer json serializer
type JSONSerializer struct{}

// Scan implements serializer interface
func (JSONSerializer) Scan(ctx context.Context, field *Field, dst reflect.Value, dbValue interface{}) (err error) {
	fieldValue := reflect.New(field.FieldType) // new 一个字段值

	if dbValue != nil { // 非空的话，将 []byte 或者 string 转为 []byte ,然后 json 反序列化
		var bytes []byte
		switch v := dbValue.(type) {
		case []byte:
			bytes = v
		case string:
			bytes = []byte(v)
		default:
			return fmt.Errorf("failed to unmarshal JSONB value: %#v", dbValue)
		}

		if len(bytes) > 0 { //
			err = json.Unmarshal(bytes, fieldValue.Interface())
		}
	}

	// 将反序列化后的值 set 到 model 结构体的对应字段里面
	field.ReflectValueOf(ctx, dst).Set(fieldValue.Elem())
	return
}

// Value implements serializer interface
func (JSONSerializer) Value(ctx context.Context, field *Field, dst reflect.Value, fieldValue interface{}) (interface{}, error) {
	result, err := json.Marshal(fieldValue)
	if string(result) == "null" { // 如果属性值是空的
		if field.TagSettings["NOT NULL"] != "" { // 带有 NOT NULL 设置的字段，返回一个 ""
			return "", nil
		}
		return nil, err
	}
	return string(result), err
}

// UnixSecondSerializer json serializer
type UnixSecondSerializer struct{}

// Scan implements serializer interface
func (UnixSecondSerializer) Scan(ctx context.Context, field *Field, dst reflect.Value, dbValue interface{}) (err error) {
	t := sql.NullTime{}
	if err = t.Scan(dbValue); err == nil && t.Valid {
		err = field.Set(ctx, dst, t.Time.Unix())
	}

	return
}

// Value implements serializer interface
func (UnixSecondSerializer) Value(ctx context.Context, field *Field, dst reflect.Value, fieldValue interface{}) (result interface{}, err error) {
	rv := reflect.ValueOf(fieldValue)
	switch v := fieldValue.(type) {
	case int64, int, uint, uint64, int32, uint32, int16, uint16:
		result = time.Unix(reflect.Indirect(rv).Int(), 0)
	case *int64, *int, *uint, *uint64, *int32, *uint32, *int16, *uint16:
		if rv.IsZero() {
			return nil, nil // 避免 *int 等指针属性经过了 gorm 后由 nil 变成 0
		}
		result = time.Unix(reflect.Indirect(rv).Int(), 0)
	default:
		err = fmt.Errorf("invalid field type %#v for UnixSecondSerializer, only int, uint supported", v)
	}
	return
}

// GobSerializer gob serializer
type GobSerializer struct{}

// Scan implements serializer interface
func (GobSerializer) Scan(ctx context.Context, field *Field, dst reflect.Value, dbValue interface{}) (err error) {
	fieldValue := reflect.New(field.FieldType)

	if dbValue != nil {
		var bytesValue []byte
		switch v := dbValue.(type) {
		case []byte:
			bytesValue = v
		default:
			return fmt.Errorf("failed to unmarshal gob value: %#v", dbValue)
		}
		if len(bytesValue) > 0 {
			decoder := gob.NewDecoder(bytes.NewBuffer(bytesValue))
			err = decoder.Decode(fieldValue.Interface())
		}
	}
	field.ReflectValueOf(ctx, dst).Set(fieldValue.Elem())
	return
}

// Value implements serializer interface
func (GobSerializer) Value(ctx context.Context, field *Field, dst reflect.Value, fieldValue interface{}) (interface{}, error) {
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(buf).Encode(fieldValue)
	return buf.Bytes(), err
}
