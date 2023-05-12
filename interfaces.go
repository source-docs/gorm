package gorm

import (
	"context"
	"database/sql"

	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// Dialector GORM database dialector
type Dialector interface {
	// Name 返回使用该 Dialector 实例连接的数据库类型的名称，例如 "mysql"、"sqlite" 等。
	Name() string
	// Initialize 用于初始化连接到数据库的 *DB 实例。此方法将在 Open 方法中调用。
	Initialize(*DB) error
	// Migrator 返回用于执行数据库迁移的 Migrator 接口实例。
	Migrator(db *DB) Migrator
	// DataTypeOf 返回给定 schema.Field 类型的数据库原生数据类型。例如，schema.Field 类型 string 可能映射到数据库中的 VARCHAR 类型。
	DataTypeOf(*schema.Field) string
	// DefaultValueOf 返回给定 schema.Field 类型的默认值表达式。如果该字段没有默认值，则返回 nil。
	DefaultValueOf(*schema.Field) clause.Expression
	// BindVarTo BindVarTo 将给定的值绑定到 SQL 语句中的占位符。
	// 例如，BindVarTo 方法可以将值 42 绑定到 SQL 语句 SELECT * FROM users WHERE age = ? 中的 ? 占位符上。
	BindVarTo(writer clause.Writer, stmt *Statement, v interface{})
	// QuoteTo 将给定的标识符（例如表名、列名等）引用为数据库原生的语法。例如，在 MySQL 中引用表名 users 可能需要将其引用为 `users`。
	QuoteTo(clause.Writer, string)
	// Explain 返回用于解释和优化 SQL 语句的字符串，可以用于调试和优化 SQL 查询。
	Explain(sql string, vars ...interface{}) string
}

// Plugin GORM plugin interface
type Plugin interface {
	Name() string
	Initialize(*DB) error
}

// ConnPool db conns pool interface
type ConnPool interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// SavePointerDialectorInterface save pointer interface
type SavePointerDialectorInterface interface {
	SavePoint(tx *DB, name string) error
	RollbackTo(tx *DB, name string) error
}

// TxBeginner tx beginner
type TxBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// ConnPoolBeginner conn pool beginner
type ConnPoolBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (ConnPool, error)
}

// TxCommitter tx committer
type TxCommitter interface {
	Commit() error
	Rollback() error
}

// Tx sql.Tx interface
type Tx interface {
	ConnPool
	TxCommitter
	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt
}

// Valuer gorm valuer interface
type Valuer interface {
	GormValue(context.Context, *DB) clause.Expr
}

// GetDBConnector SQL db connector
type GetDBConnector interface {
	GetDBConn() (*sql.DB, error)
}

// Rows rows interface
type Rows interface {
	Columns() ([]string, error)
	ColumnTypes() ([]*sql.ColumnType, error)
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
	Close() error
}
