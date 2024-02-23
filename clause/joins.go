package clause

type JoinType string

const (
	CrossJoin JoinType = "CROSS"
	InnerJoin JoinType = "INNER"
	LeftJoin  JoinType = "LEFT"
	RightJoin JoinType = "RIGHT"
)

// Join clause for from
type Join struct {
	Type  JoinType
	Table Table
	// join on 条件
	ON Where
	// join 的 using 选项
	Using []string

	// Expression 嵌套的其他 Expression， 比如 raw SQL 模式的 NamedExpr
	Expression Expression
}

func (join Join) Build(builder Builder) {
	if join.Expression != nil {
		join.Expression.Build(builder)
	} else {
		if join.Type != "" { // 指定了 join 类型
			builder.WriteString(string(join.Type))
			builder.WriteByte(' ')
		}

		builder.WriteString("JOIN ")
		builder.WriteQuoted(join.Table)

		if len(join.ON.Exprs) > 0 {
			// 指定了 join on 条件
			builder.WriteString(" ON ")
			join.ON.Build(builder)
		} else if len(join.Using) > 0 {
			// join 的 using 选项
			builder.WriteString(" USING (")
			for idx, c := range join.Using {
				if idx > 0 {
					builder.WriteByte(',')
				}
				builder.WriteQuoted(c)
			}
			builder.WriteByte(')')
		}
	}
}
