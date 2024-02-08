package clause

type Insert struct {
	// 需要插入的表名，如果为空，使用 statement 表名
	Table Table
	// Insert 后面的选项，如 DELAYED, HIGH_PRIORITY, IGNORE,
	Modifier string
}

// Name insert clause name
func (insert Insert) Name() string {
	return "INSERT"
}

// Build build insert clause
func (insert Insert) Build(builder Builder) {
	// 插入 Insert 选项
	if insert.Modifier != "" {
		builder.WriteString(insert.Modifier)
		builder.WriteByte(' ')
	}

	builder.WriteString("INTO ")
	if insert.Table.Name == "" {
		builder.WriteQuoted(currentTable)
	} else {
		builder.WriteQuoted(insert.Table)
	}
}

// MergeClause merge insert clause
func (insert Insert) MergeClause(clause *Clause) {
	if v, ok := clause.Expression.(Insert); ok {
		if insert.Modifier == "" {
			insert.Modifier = v.Modifier
		}
		if insert.Table.Name == "" {
			insert.Table = v.Table
		}
	}
	clause.Expression = insert
}
