package clause

import (
	"strings"
)

const (
	AndWithSpace = " AND "
	OrWithSpace  = " OR "
)

// Where where clause
type Where struct {
	Exprs []Expression // Where 的条件
}

// Name where clause name
func (where Where) Name() string {
	return "WHERE"
}

// Build build where clause
func (where Where) Build(builder Builder) {
	// Switch position if the first query expression is a single Or condition
	for idx, expr := range where.Exprs {
		if v, ok := expr.(OrConditions); !ok || len(v.Exprs) > 1 { // 如果 条件不是 or 或者 exprs > 1
			if idx != 0 { // 如果 不是第一个，放第一个
				where.Exprs[0], where.Exprs[idx] = where.Exprs[idx], where.Exprs[0]
			}
			break
		}
	}

	buildExprs(where.Exprs, builder, AndWithSpace) // where 条件之间的连接使用 AND
}

// buildExprs 构建 where 子句的 sql
// joinCond 条件之间的逻辑连接符号，and 或者 or
func buildExprs(exprs []Expression, builder Builder, joinCond string) {
	wrapInParentheses := false // 该条件是否需要以括号包裹

	for idx, expr := range exprs {
		if idx > 0 { // 第二个条件开始需要判断多条件直接的逻辑连接
			if v, ok := expr.(OrConditions); ok && len(v.Exprs) == 1 {
				// 如果条件是 or, 并且表达式大于 1 个， 使用 or 连接
				builder.WriteString(OrWithSpace)
			} else {
				builder.WriteString(joinCond) // 其他情况用指定的条件连接
			}
		}

		// 如果存在多个条件，需要逻辑连接
		if len(exprs) > 1 {

			switch v := expr.(type) {
			case OrConditions:
				// 如果 Where.Exprs 是一个 OrConditions 的 Expression

				if len(v.Exprs) == 1 { // 如果是 raw SQL
					if e, ok := v.Exprs[0].(Expr); ok {
						sql := strings.ToUpper(e.SQL)
						// 如果 or 表达式只有一个条件，构建 OrConditions 时没有加前后括号
						// 这里有多个条件表达式，需要加括号
						wrapInParentheses = strings.Contains(sql, AndWithSpace) || strings.Contains(sql, OrWithSpace)
					}
				}
			case AndConditions:
				// 如果 Where.Exprs 是一个 AndConditions 的 Expression
				if len(v.Exprs) == 1 {
					// 如果是 raw SQL
					if e, ok := v.Exprs[0].(Expr); ok {
						sql := strings.ToUpper(e.SQL)
						// 和 or 一样，AndConditions.Exprs 为1时，没有加前后括号，这里补一下
						wrapInParentheses = strings.Contains(sql, AndWithSpace) || strings.Contains(sql, OrWithSpace)
					}
				}
			case Expr:
				// 如果 Where.Exprs 是一个 Expr （raw SQL）
				sql := strings.ToUpper(v.SQL)
				// Raw SQL 里面存在 and 或者 or 就需要加前后括号
				wrapInParentheses = strings.Contains(sql, AndWithSpace) || strings.Contains(sql, OrWithSpace)
			case NamedExpr: // 命名参数
				sql := strings.ToUpper(v.SQL)
				// SQL 里面存在 and 或者 or 就需要加前后括号
				wrapInParentheses = strings.Contains(sql, AndWithSpace) || strings.Contains(sql, OrWithSpace)
			}
		}

		// 加前后括号
		if wrapInParentheses {
			builder.WriteByte('(')
			expr.Build(builder)
			builder.WriteByte(')')
			wrapInParentheses = false
		} else {
			expr.Build(builder)
		}
	}
}

// MergeClause merge where clauses
func (where Where) MergeClause(clause *Clause) {
	if w, ok := clause.Expression.(Where); ok {
		exprs := make([]Expression, len(w.Exprs)+len(where.Exprs))
		copy(exprs, w.Exprs)
		copy(exprs[len(w.Exprs):], where.Exprs)
		where.Exprs = exprs
	}

	clause.Expression = where
}

func And(exprs ...Expression) Expression {
	if len(exprs) == 0 {
		return nil
	}

	if len(exprs) == 1 {
		if _, ok := exprs[0].(OrConditions); !ok {
			return exprs[0]
		}
	}

	return AndConditions{Exprs: exprs}
}

type AndConditions struct {
	Exprs []Expression
}

func (and AndConditions) Build(builder Builder) {
	if len(and.Exprs) > 1 {
		builder.WriteByte('(')
		buildExprs(and.Exprs, builder, AndWithSpace)
		builder.WriteByte(')')
	} else {
		buildExprs(and.Exprs, builder, AndWithSpace)
	}
}

func Or(exprs ...Expression) Expression {
	if len(exprs) == 0 {
		return nil
	}
	return OrConditions{Exprs: exprs}
}

type OrConditions struct {
	Exprs []Expression
}

func (or OrConditions) Build(builder Builder) {
	if len(or.Exprs) > 1 {
		builder.WriteByte('(')
		buildExprs(or.Exprs, builder, OrWithSpace)
		builder.WriteByte(')')
	} else {
		buildExprs(or.Exprs, builder, OrWithSpace)
	}
}

func Not(exprs ...Expression) Expression {
	if len(exprs) == 0 {
		return nil
	}
	return NotConditions{Exprs: exprs}
}

type NotConditions struct {
	Exprs []Expression
}

func (not NotConditions) Build(builder Builder) {
	if len(not.Exprs) > 1 {
		builder.WriteByte('(')
	}

	for idx, c := range not.Exprs {
		if idx > 0 {
			builder.WriteString(AndWithSpace)
		}

		if negationBuilder, ok := c.(NegationExpressionBuilder); ok {
			negationBuilder.NegationBuild(builder)
		} else {
			builder.WriteString("NOT ")
			e, wrapInParentheses := c.(Expr)
			if wrapInParentheses {
				sql := strings.ToUpper(e.SQL)
				if wrapInParentheses = strings.Contains(sql, AndWithSpace) || strings.Contains(sql, OrWithSpace); wrapInParentheses {
					builder.WriteByte('(')
				}
			}

			c.Build(builder)

			if wrapInParentheses {
				builder.WriteByte(')')
			}
		}
	}

	if len(not.Exprs) > 1 {
		builder.WriteByte(')')
	}
}
