package clause

// From from clause
type From struct {
	// 数据来源表
	Tables []Table
	// 嵌套的 Join 子句
	Joins []Join
}

// Name from clause name
func (from From) Name() string {
	return "FROM"
}

// Build build from clause
func (from From) Build(builder Builder) {
	if len(from.Tables) > 0 { // 多表，使用 , 分隔
		for idx, table := range from.Tables {
			if idx > 0 {
				builder.WriteByte(',')
			}

			builder.WriteQuoted(table)
		}
	} else {
		builder.WriteQuoted(currentTable) // 默认情况下，写入当前表占位符
	}

	for _, join := range from.Joins { // from 带 join
		builder.WriteByte(' ')
		join.Build(builder)
	}
}

// MergeClause merge from clause
func (from From) MergeClause(clause *Clause) {
	clause.Expression = from
}
