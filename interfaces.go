package gorm

import (
	"context"
	"database/sql"

	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// Dialector GORM database dialector
// 实现数据库的驱动程序和数据库方言。
type Dialector interface {
	// Name 返回使用该 Dialector 实例连接的数据库类型的名称，例如 "mysql"、"sqlite" 等。
	Name() string
	// Initialize 用于初始化连接到数据库的 *DB 实例。此方法将在 Open 方法中调用。发起连接，并且注册 callback
	Initialize(*DB) error
	// Migrator 返回用于执行数据库迁移的 Migrator 接口实例, 用于管理数据库迁移。该接口主要用于执行和管理数据模型和数据表之间的映射关系。
	Migrator(db *DB) Migrator
	// DataTypeOf 返回给定 schema.Field 类型的数据库原生数据类型, 该方法通常在需要映射数据模型和数据库类型时使用。。例如，schema.Field 类型 string 可能映射到数据库中的 VARCHAR 类型。
	DataTypeOf(*schema.Field) string
	// DefaultValueOf 返回给定 schema.Field 类型的默认值表达式, 该方法通常在需要设置数据模型字段默认值时使用。如果该字段没有默认值，则返回 nil。
	DefaultValueOf(*schema.Field) clause.Expression
	// BindVarTo BindVarTo 将给定的值绑定到 SQL 语句中的占位符。该方法通常用于构建动态 SQL 语句。
	// 例如，BindVarTo 方法可以将值 42 绑定到 SQL 语句 SELECT * FROM users WHERE age = ? 中的 ? 占位符上。
	BindVarTo(writer clause.Writer, stmt *Statement, v interface{})
	// QuoteTo 将给定的标识符（例如表名、列名等）引用为数据库原生的语法, 该方法通常用于保证 SQL 语句的安全性和正确性。。例如，在 MySQL 中引用表名 users 可能需要将其引用为 `users`。
	QuoteTo(clause.Writer, string)
	// Explain 生成一条解释 SQL 执行计划的 SQL 语句。该方法通常用于优化数据库的查询性能和调试 SQL 语句。
	Explain(sql string, vars ...interface{}) string
}

// Plugin GORM plugin interface
type Plugin interface {
	Name() string
	// Initialize 插件初始化， Open 函数执行完调用
	Initialize(*DB) error
}

type ParamsFilter interface {
	ParamsFilter(ctx context.Context, sql string, params ...interface{}) (string, []interface{})
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

type ErrorTranslator interface {
	Translate(err error) error
}
