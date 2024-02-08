package clause

// Delete 子句实现
type Delete struct {
	// Modifier 删除的表名
	Modifier string
}

func (d Delete) Name() string {
	return "DELETE"
}

func (d Delete) Build(builder Builder) {
	builder.WriteString("DELETE")

	if d.Modifier != "" {
		builder.WriteByte(' ')
		builder.WriteString(d.Modifier)
	}
}

func (d Delete) MergeClause(clause *Clause) {
	clause.Name = ""
	clause.Expression = d
}
