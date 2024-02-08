package clause

import (
	"database/sql"
	"database/sql/driver"
	"go/ast"
	"reflect"
)

// Expression expression interface
type Expression interface {
	Build(builder Builder)
}

// NegationExpressionBuilder negation expression builder
type NegationExpressionBuilder interface {
	NegationBuild(builder Builder)
}

// Expr raw expression
// Rwa SQL 表达式
type Expr struct {
	SQL  string
	Vars []interface{}
	// 将 sql 里面的 ? 当做 (?) 解析，支持子表达式或者列表
	WithoutParentheses bool
}

// Build build raw expression
func (expr Expr) Build(builder Builder) {
	var (
		afterParenthesis bool // 标记原始 sql 里面，当前字符的上一个字符是否是 (
		idx              int  // sql 里面有 ? 时，当前 vars 匹配到了第几个
	)

	for _, v := range []byte(expr.SQL) {
		c := string([]byte{v})
		_ = c                                 // 只用于方便调试
		if v == '?' && len(expr.Vars) > idx { // 遇到 ? 并且 vars 还没用完
			if afterParenthesis || expr.WithoutParentheses {
				if _, ok := expr.Vars[idx].(driver.Valuer); ok {
					builder.AddVar(builder, expr.Vars[idx])
				} else {
					switch rv := reflect.ValueOf(expr.Vars[idx]); rv.Kind() {
					case reflect.Slice, reflect.Array:
						if rv.Len() == 0 {
							builder.AddVar(builder, nil)
						} else {
							for i := 0; i < rv.Len(); i++ {
								if i > 0 {
									builder.WriteByte(',')
								}
								builder.AddVar(builder, rv.Index(i).Interface())
							}
						}
					default:
						builder.AddVar(builder, expr.Vars[idx])
					}
				}
			} else { // 普通变量
				builder.AddVar(builder, expr.Vars[idx])
			}

			idx++
		} else { // 普通字符
			if v == '(' {
				afterParenthesis = true
			} else {
				afterParenthesis = false
			}
			builder.WriteByte(v)
		}
	}

	if idx < len(expr.Vars) {
		for _, v := range expr.Vars[idx:] {
			builder.AddVar(builder, sql.NamedArg{Value: v})
		}
	}
}

// NamedExpr raw expression for named expr
// 带有命名参数的表达式, 比如：@name
type NamedExpr struct {
	SQL  string
	Vars []interface{}
}

// Build build raw expression
func (expr NamedExpr) Build(builder Builder) {
	var (
		idx              int // sql 里面有 ? 时，当前 vars 匹配到了第几个
		inName           bool
		afterParenthesis bool                                           // 标记原始 sql 里面，当前字符的上一个字符是否是 (
		namedMap         = make(map[string]interface{}, len(expr.Vars)) // 命名参数以及对应的值
	)

	for _, v := range expr.Vars {
		switch value := v.(type) {
		case sql.NamedArg:
			// 命名参数以减少在使用参数过程中出错
			namedMap[value.Name] = value.Value
		case map[string]interface{}:
			// map 也可以实现命名参数，使用 map 可以一次性添加多个 命名参数
			for k, v := range value {
				namedMap[k] = v
			}
		default:
			var appendFieldsToMap func(reflect.Value)
			appendFieldsToMap = func(reflectValue reflect.Value) {
				reflectValue = reflect.Indirect(reflectValue)
				switch reflectValue.Kind() {
				case reflect.Struct: // 如果是结构体，
					modelType := reflectValue.Type()
					for i := 0; i < modelType.NumField(); i++ {
						// 遍历所有字段
						if fieldStruct := modelType.Field(i); ast.IsExported(fieldStruct.Name) {
							// 筛选出所有导出字段
							// 将结构体的字段作为 key, 值作为 value, 放入 命名参数表里面
							namedMap[fieldStruct.Name] = reflectValue.Field(i).Interface()

							if fieldStruct.Anonymous {
								// 如果是嵌入结构体
								appendFieldsToMap(reflectValue.Field(i)) // 对该结构体进行递归遍历，添加字段
							}
						}
					}
				}
			}

			appendFieldsToMap(reflect.ValueOf(value))
		}
	}

	name := make([]byte, 0, 10)

	for _, v := range []byte(expr.SQL) {
		c := string([]byte{v})
		_ = c // 只用于方便调试
		if v == '@' && !inName {
			// @ 表示命名参数开始，多个 @ 会将后面的 @ 当做名字的一部分
			inName = true // 开始读取一个命名参数
			name = []byte{}
		} else if v == ' ' || v == ',' || v == ')' || v == '"' || v == '\'' || v == '`' || v == '\r' || v == '\n' || v == ';' {
			// 这些特殊字符作为变量的分隔符
			if inName { // 如果刚刚读取完成的是一个命名参数
				if nv, ok := namedMap[string(name)]; ok {
					// 如果这个命名参数在 namedMap 里面可以找到
					builder.AddVar(builder, nv) // sql 里面填 ？， values 里面加值
				} else {
					// 如果找不到，再把 @{name} 原样写进去
					builder.WriteByte('@')
					builder.WriteString(string(name))
				}
				inName = false
			}

			afterParenthesis = false
			builder.WriteByte(v)
		} else if v == '?' && len(expr.Vars) > idx {
			if afterParenthesis {
				// 上一个字符是 (
				if _, ok := expr.Vars[idx].(driver.Valuer); ok {
					builder.AddVar(builder, expr.Vars[idx])
				} else {
					switch rv := reflect.ValueOf(expr.Vars[idx]); rv.Kind() {
					case reflect.Slice, reflect.Array:
						if rv.Len() == 0 {
							builder.AddVar(builder, nil)
						} else {
							// 是 list
							for i := 0; i < rv.Len(); i++ {
								// 遍历 list 添加 多个 ?
								if i > 0 {
									builder.WriteByte(',')
								}
								builder.AddVar(builder, rv.Index(i).Interface())
							}
						}
					default: // 不是 list, 直接添加 var
						builder.AddVar(builder, expr.Vars[idx])
					}
				}
			} else { // 上一个字符不是 (
				builder.AddVar(builder, expr.Vars[idx])
			}

			idx++
		} else if inName { // 正在读取命名参数
			name = append(name, v)
		} else { // 普通字符
			if v == '(' {
				afterParenthesis = true // 标记上一个字符是 (
			} else {
				afterParenthesis = false
			}
			builder.WriteByte(v)
		}
	}

	if inName { // 如命名参数在最后位置
		if nv, ok := namedMap[string(name)]; ok {
			// 找到添加到 values 里面
			builder.AddVar(builder, nv)
		} else { // 找不到原样写回
			builder.WriteByte('@')
			builder.WriteString(string(name))
		}
	}
}

// IN Whether a value is within a set of values
type IN struct {
	Column interface{}
	Values []interface{}
}

func (in IN) Build(builder Builder) {
	builder.WriteQuoted(in.Column)

	switch len(in.Values) {
	case 0:
		builder.WriteString(" IN (NULL)")
	case 1:
		if _, ok := in.Values[0].([]interface{}); !ok {
			builder.WriteString(" = ")
			builder.AddVar(builder, in.Values[0])
			break
		}

		fallthrough
	default:
		builder.WriteString(" IN (")
		builder.AddVar(builder, in.Values...)
		builder.WriteByte(')')
	}
}

func (in IN) NegationBuild(builder Builder) {
	builder.WriteQuoted(in.Column)
	switch len(in.Values) {
	case 0:
		builder.WriteString(" IS NOT NULL")
	case 1:
		if _, ok := in.Values[0].([]interface{}); !ok {
			builder.WriteString(" <> ")
			builder.AddVar(builder, in.Values[0])
			break
		}

		fallthrough
	default:
		builder.WriteString(" NOT IN (")
		builder.AddVar(builder, in.Values...)
		builder.WriteByte(')')
	}
}

// Eq equal to for where
type Eq struct {
	Column interface{} // 行号
	Value  interface{} // 相等的值
}

func (eq Eq) Build(builder Builder) {
	builder.WriteQuoted(eq.Column)

	switch eq.Value.(type) {
	case []string, []int, []int32, []int64, []uint, []uint32, []uint64, []interface{}: // 如果 Value 是一个列表
		builder.WriteString(" IN (") // list 使用 in 构建
		rv := reflect.ValueOf(eq.Value)
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.AddVar(builder, rv.Index(i).Interface())
		}
		builder.WriteByte(')')
	default: // 非列表值
		if eqNil(eq.Value) {
			builder.WriteString(" IS NULL") // value 是 nil, 使用 is null
		} else { // 其他情况用 =
			builder.WriteString(" = ")
			builder.AddVar(builder, eq.Value)
		}
	}
}

func (eq Eq) NegationBuild(builder Builder) {
	Neq(eq).Build(builder)
}

// Neq not equal to for where
type Neq Eq

func (neq Neq) Build(builder Builder) {
	builder.WriteQuoted(neq.Column)

	switch neq.Value.(type) {
	case []string, []int, []int32, []int64, []uint, []uint32, []uint64, []interface{}:
		builder.WriteString(" NOT IN (")
		rv := reflect.ValueOf(neq.Value)
		for i := 0; i < rv.Len(); i++ {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.AddVar(builder, rv.Index(i).Interface())
		}
		builder.WriteByte(')')
	default:
		if eqNil(neq.Value) {
			builder.WriteString(" IS NOT NULL")
		} else {
			builder.WriteString(" <> ")
			builder.AddVar(builder, neq.Value)
		}
	}
}

func (neq Neq) NegationBuild(builder Builder) {
	Eq(neq).Build(builder)
}

// Gt greater than for where
type Gt Eq

func (gt Gt) Build(builder Builder) {
	builder.WriteQuoted(gt.Column)
	builder.WriteString(" > ")
	builder.AddVar(builder, gt.Value)
}

func (gt Gt) NegationBuild(builder Builder) {
	Lte(gt).Build(builder)
}

// Gte greater than or equal to for where
type Gte Eq

func (gte Gte) Build(builder Builder) {
	builder.WriteQuoted(gte.Column)
	builder.WriteString(" >= ")
	builder.AddVar(builder, gte.Value)
}

func (gte Gte) NegationBuild(builder Builder) {
	Lt(gte).Build(builder)
}

// Lt less than for where
type Lt Eq

func (lt Lt) Build(builder Builder) {
	builder.WriteQuoted(lt.Column)
	builder.WriteString(" < ")
	builder.AddVar(builder, lt.Value)
}

func (lt Lt) NegationBuild(builder Builder) {
	Gte(lt).Build(builder)
}

// Lte less than or equal to for where
type Lte Eq

func (lte Lte) Build(builder Builder) {
	builder.WriteQuoted(lte.Column)
	builder.WriteString(" <= ")
	builder.AddVar(builder, lte.Value)
}

func (lte Lte) NegationBuild(builder Builder) {
	Gt(lte).Build(builder)
}

// Like whether string matches regular expression
type Like Eq

func (like Like) Build(builder Builder) {
	builder.WriteQuoted(like.Column)
	builder.WriteString(" LIKE ")
	builder.AddVar(builder, like.Value)
}

func (like Like) NegationBuild(builder Builder) {
	builder.WriteQuoted(like.Column)
	builder.WriteString(" NOT LIKE ")
	builder.AddVar(builder, like.Value)
}

// value 是否是 nil
func eqNil(value interface{}) bool {
	if valuer, ok := value.(driver.Valuer); ok && !eqNilReflect(valuer) {
		// 如果是 driver.Valuer，调用 eqNilReflect 判断是否 nil，
		// 如果非 nil, 调用 Value 转换后判断转换后的
		value, _ = valuer.Value()
	}

	return value == nil || eqNilReflect(value)
}

// value 是指针并且等于 nil
func eqNilReflect(value interface{}) bool {
	reflectValue := reflect.ValueOf(value)
	return reflectValue.Kind() == reflect.Ptr && reflectValue.IsNil()
}
