package gorm

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// for Config.cacheStore store PreparedStmtDB key
const preparedStmtDBKey = "preparedStmt"

// Config GORM config
type Config struct {
	// GORM perform single create, update, delete operations in transactions by default to ensure database data integrity
	// You can disable it by setting `SkipDefaultTransaction` to true
	// GORM 默认的数据更新、创建都在事务中
	// GORM 在事务内执行写入（创建/更新/删除）操作以确保数据一致性，如果不需要，可以在初始化期间禁用它
	// 这里包在事务里面是为了处理hook中的err，
	// 对于没有关联，并没有在 After Hooks 中返回错误以来 rollback 整个事务情况的对象，基本可以安全的来关闭默认事务
	SkipDefaultTransaction bool
	// NamingStrategy tables, columns naming strategy
	// 自定义命名策略
	NamingStrategy schema.Namer
	// FullSaveAssociations full save associations
	FullSaveAssociations bool
	// Logger 自定义 log
	Logger logger.Interface
	// NowFunc the function to be used when creating a new timestamp
	// 创建时间戳的方法，这样可以自定义时间精度
	NowFunc func() time.Time
	// DryRun generate sql without execute
	// 只生成 sql 不运行
	DryRun bool
	// PrepareStmt executes the given query in cached statement
	// 是否使用缓存的 statement 来运行 查询
	PrepareStmt bool
	// DisableAutomaticPing 禁止 open 之后发送一个 ping
	DisableAutomaticPing bool
	// DisableForeignKeyConstraintWhenMigrating
	DisableForeignKeyConstraintWhenMigrating bool
	// IgnoreRelationshipsWhenMigrating
	IgnoreRelationshipsWhenMigrating bool
	// DisableNestedTransaction disable nested transaction
	DisableNestedTransaction bool
	// AllowGlobalUpdate allow global update
	// 允许没有 where 条件的全表更新
	AllowGlobalUpdate bool
	// QueryFields executes the SQL query with all fields of the table
	QueryFields bool
	// CreateBatchSize default create batch size 分批创建的时候，每批大小
	CreateBatchSize int
	// TranslateError enabling error translation
	TranslateError bool

	// ClauseBuilders clause builder
	// 子句构建器，可以覆盖子句默认实现
	ClauseBuilders map[string]clause.ClauseBuilder
	// ConnPool db conn pool， 具体的连接池，如 sql.Open 返回的连接池
	ConnPool ConnPool
	// Dialector database dialector 方言，每种 sql 的具体实现
	Dialector
	// Plugins registered plugins
	Plugins map[string]Plugin

	callbacks *callbacks
	// 缓存，用于缓存解析好的 Schema，也会用来缓存 preparedStmtDBKey 或者  embeddedCacheKey
	cacheStore *sync.Map
}

// Apply update config to new config
func (c *Config) Apply(config *Config) error {
	if config != c {
		*config = *c
	}
	return nil
}

// AfterInitialize initialize plugins after db connected
// AfterInitialize 在 DB 初始化后调用, 这里用来初始化插件
func (c *Config) AfterInitialize(db *DB) error {
	if db != nil {
		for _, plugin := range c.Plugins {
			if err := plugin.Initialize(db); err != nil {
				return err
			}
		}
	}
	return nil
}

// Option gorm option interface
type Option interface {
	Apply(*Config) error
	// AfterInitialize 在 DB 初始化后调用
	AfterInitialize(*DB) error
}

// DB GORM DB definition
type DB struct {
	*Config
	// 处理过程中产生的错误
	Error error
	// 运行 sql 影响的行数
	RowsAffected int64
	Statement    *Statement
	// 如果 clone == 0; getInstance 的时候, 直接返回
	// 如果 clone == 1: getInstance 的时候， statement 用全新的，只继承一些必要数据
	// 如果 clone == 2: getInstance 的时候， 继承之前的 Statement 副本
	clone int
}

// Session session config when create session with Session() method
type Session struct {
	// 只生成 sql 不运行
	DryRun bool
	// 是否使用缓存的 statement 来运行 查询
	PrepareStmt bool
	// 是否需要新建 statement， clone = 2
	NewDB bool
	// 是否已初始化，跳过 tx.getInstance()
	Initialized              bool
	SkipHooks                bool
	SkipDefaultTransaction   bool
	DisableNestedTransaction bool
	// 允许没有 where 条件的全表更新
	AllowGlobalUpdate    bool
	FullSaveAssociations bool
	QueryFields          bool
	Context              context.Context
	Logger               logger.Interface
	NowFunc              func() time.Time
	CreateBatchSize      int
}

// Open initialize db session based on dialector
// 打开连接
func Open(dialector Dialector, opts ...Option) (db *DB, err error) {
	config := &Config{}

	// 将类型为 *Config 的 Option 排在最前面
	sort.Slice(opts, func(i, j int) bool {
		_, isConfig := opts[i].(*Config)
		_, isConfig2 := opts[j].(*Config)
		return isConfig && !isConfig2
	})

	for _, opt := range opts {
		if opt != nil {
			if applyErr := opt.Apply(config); applyErr != nil {
				return nil, applyErr
			}
			defer func(opt Option) {
				// 在 db 初始化后调用，例如初始化插件
				if errr := opt.AfterInitialize(db); errr != nil {
					err = errr
				}
			}(opt)
		}
	}

	// 如果 dialector 也实现了 Apply 方法，也会调用
	if d, ok := dialector.(interface{ Apply(*Config) error }); ok {
		if err = d.Apply(config); err != nil {
			return
		}
	}

	if config.NamingStrategy == nil { // 设置自定义命名策略
		config.NamingStrategy = schema.NamingStrategy{}
	}

	// 自定义 logger
	if config.Logger == nil {
		config.Logger = logger.Default
	}

	// 自定义 创建时间戳的方法，这样可以自定义时间精度
	if config.NowFunc == nil {
		config.NowFunc = func() time.Time { return time.Now().Local() } // 默认使用最高精度的碧迪时间
	}

	// 设置 sql 方言实现
	if dialector != nil {
		config.Dialector = dialector
	}

	if config.Plugins == nil {
		config.Plugins = map[string]Plugin{}
	}

	if config.cacheStore == nil {
		config.cacheStore = &sync.Map{}
	}

	db = &DB{Config: config, clone: 1}

	db.callbacks = initializeCallbacks(db) // 初始化 callbacks 的数据结构

	if config.ClauseBuilders == nil {
		config.ClauseBuilders = map[string]clause.ClauseBuilder{}
	}

	if config.Dialector != nil {
		// 初始化具体类型 sql 实现的连接，并且注册 callback
		err = config.Dialector.Initialize(db)

		if err != nil {
			if db, err := db.DB(); err == nil {
				_ = db.Close()
			}
		}
	}

	preparedStmt := &PreparedStmtDB{
		ConnPool:    db.ConnPool,
		Stmts:       make(map[string]*Stmt),
		Mux:         &sync.RWMutex{},
		PreparedSQL: make([]string, 0, 100),
	}
	db.cacheStore.Store(preparedStmtDBKey, preparedStmt)

	if config.PrepareStmt {
		db.ConnPool = preparedStmt
	}

	db.Statement = &Statement{
		DB:       db,
		ConnPool: db.ConnPool,
		Context:  context.Background(),
		Clauses:  map[string]clause.Clause{},
	}

	if err == nil && !config.DisableAutomaticPing { // 如果没有关闭自动 ping，并且连接池实现了 ping 方法
		if pinger, ok := db.ConnPool.(interface{ Ping() error }); ok {
			err = pinger.Ping()
		}
	}

	if err != nil {
		config.Logger.Error(context.Background(), "failed to initialize database, got error %v", err)
	}

	return
}

// Session create new db session
func (db *DB) Session(config *Session) *DB {
	var (
		txConfig = *db.Config
		tx       = &DB{
			Config:    &txConfig,
			Statement: db.Statement,
			Error:     db.Error,
			clone:     1,
		}
	)
	if config.CreateBatchSize > 0 {
		tx.Config.CreateBatchSize = config.CreateBatchSize
	}

	if config.SkipDefaultTransaction {
		tx.Config.SkipDefaultTransaction = true
	}

	if config.AllowGlobalUpdate {
		txConfig.AllowGlobalUpdate = true
	}

	if config.FullSaveAssociations {
		txConfig.FullSaveAssociations = true
	}

	if config.Context != nil || config.PrepareStmt || config.SkipHooks {
		tx.Statement = tx.Statement.clone()
		tx.Statement.DB = tx
	}

	if config.Context != nil {
		tx.Statement.Context = config.Context
	}

	if config.PrepareStmt {
		if v, ok := db.cacheStore.Load(preparedStmtDBKey); ok {
			preparedStmt := v.(*PreparedStmtDB)
			switch t := tx.Statement.ConnPool.(type) {
			case Tx:
				tx.Statement.ConnPool = &PreparedStmtTX{
					Tx:             t,
					PreparedStmtDB: preparedStmt,
				}
			default:
				tx.Statement.ConnPool = &PreparedStmtDB{
					ConnPool: db.Config.ConnPool,
					Mux:      preparedStmt.Mux,
					Stmts:    preparedStmt.Stmts,
				}
			}
			txConfig.ConnPool = tx.Statement.ConnPool
			txConfig.PrepareStmt = true
		}
	}

	if config.SkipHooks {
		tx.Statement.SkipHooks = true
	}

	if config.DisableNestedTransaction {
		txConfig.DisableNestedTransaction = true
	}

	if !config.NewDB {
		tx.clone = 2
	}

	if config.DryRun {
		tx.Config.DryRun = true
	}

	if config.QueryFields {
		tx.Config.QueryFields = true
	}

	if config.Logger != nil {
		tx.Config.Logger = config.Logger
	}

	if config.NowFunc != nil {
		tx.Config.NowFunc = config.NowFunc
	}

	if config.Initialized {
		tx = tx.getInstance()
	}

	return tx
}

// WithContext change current instance db's context to ctx
func (db *DB) WithContext(ctx context.Context) *DB {
	return db.Session(&Session{Context: ctx})
}

// Debug start debug mode
func (db *DB) Debug() (tx *DB) {
	tx = db.getInstance()
	return tx.Session(&Session{
		Logger: db.Logger.LogMode(logger.Info),
	})
}

// Set store value with key into current db instance's context
func (db *DB) Set(key string, value interface{}) *DB {
	tx := db.getInstance()
	tx.Statement.Settings.Store(key, value)
	return tx
}

// Get get value with key from current db instance's context
func (db *DB) Get(key string) (interface{}, bool) {
	return db.Statement.Settings.Load(key)
}

// InstanceSet store value with key into current db instance's context
func (db *DB) InstanceSet(key string, value interface{}) *DB {
	tx := db.getInstance()
	tx.Statement.Settings.Store(fmt.Sprintf("%p", tx.Statement)+key, value)
	return tx
}

// InstanceGet get value with key from current db instance's context
func (db *DB) InstanceGet(key string) (interface{}, bool) {
	return db.Statement.Settings.Load(fmt.Sprintf("%p", db.Statement) + key)
}

// Callback returns callback manager
func (db *DB) Callback() *callbacks {
	return db.callbacks
}

// AddError add error to db
func (db *DB) AddError(err error) error {
	if err != nil {
		if db.Config.TranslateError {
			if errTranslator, ok := db.Dialector.(ErrorTranslator); ok {
				err = errTranslator.Translate(err)
			}
		}

		if db.Error == nil {
			db.Error = err
		} else {
			db.Error = fmt.Errorf("%v; %w", db.Error, err)
		}
	}
	return db.Error
}

// DB returns `*sql.DB`
func (db *DB) DB() (*sql.DB, error) {
	connPool := db.ConnPool

	if dbConnector, ok := connPool.(GetDBConnector); ok && dbConnector != nil {
		return dbConnector.GetDBConn()
	}

	if sqldb, ok := connPool.(*sql.DB); ok {
		return sqldb, nil
	}

	return nil, ErrInvalidDB
}

// 处理 db 的
func (db *DB) getInstance() *DB {
	if db.clone > 0 {
		tx := &DB{Config: db.Config, Error: db.Error}

		if db.clone == 1 {
			// clone with new statement
			// statement 用全新的，只继承一些必要数据
			tx.Statement = &Statement{
				DB:       tx,
				ConnPool: db.Statement.ConnPool,
				Context:  db.Statement.Context,
				Clauses:  map[string]clause.Clause{},
				Vars:     make([]interface{}, 0, 8),
			}
		} else {
			// 继承之前的 Statement 副本
			// with clone statement
			tx.Statement = db.Statement.clone()
			tx.Statement.DB = tx
		}

		return tx
	}

	return db
}

// Expr returns clause.Expr, which can be used to pass SQL expression as params
func Expr(expr string, args ...interface{}) clause.Expr {
	return clause.Expr{SQL: expr, Vars: args}
}

// SetupJoinTable setup join table schema
func (db *DB) SetupJoinTable(model interface{}, field string, joinTable interface{}) error {
	var (
		tx                      = db.getInstance()
		stmt                    = tx.Statement
		modelSchema, joinSchema *schema.Schema
	)

	err := stmt.Parse(model)
	if err != nil {
		return err
	}
	modelSchema = stmt.Schema

	err = stmt.Parse(joinTable)
	if err != nil {
		return err
	}
	joinSchema = stmt.Schema

	relation, ok := modelSchema.Relationships.Relations[field]
	isRelation := ok && relation.JoinTable != nil
	if !isRelation {
		return fmt.Errorf("failed to find relation: %s", field)
	}

	for _, ref := range relation.References {
		f := joinSchema.LookUpField(ref.ForeignKey.DBName)
		if f == nil {
			return fmt.Errorf("missing field %s for join table", ref.ForeignKey.DBName)
		}

		f.DataType = ref.ForeignKey.DataType
		f.GORMDataType = ref.ForeignKey.GORMDataType
		if f.Size == 0 {
			f.Size = ref.ForeignKey.Size
		}
		ref.ForeignKey = f
	}

	for name, rel := range relation.JoinTable.Relationships.Relations {
		if _, ok := joinSchema.Relationships.Relations[name]; !ok {
			rel.Schema = joinSchema
			joinSchema.Relationships.Relations[name] = rel
		}
	}
	relation.JoinTable = joinSchema

	return nil
}

// Use use plugin
func (db *DB) Use(plugin Plugin) error {
	name := plugin.Name()
	if _, ok := db.Plugins[name]; ok {
		return ErrRegistered
	}
	if err := plugin.Initialize(db); err != nil {
		return err
	}
	db.Plugins[name] = plugin
	return nil
}

// ToSQL for generate SQL string.
//
//	db.ToSQL(func(tx *gorm.DB) *gorm.DB {
//			return tx.Model(&User{}).Where(&User{Name: "foo", Age: 20})
//				.Limit(10).Offset(5)
//				.Order("name ASC")
//				.First(&User{})
//	})
func (db *DB) ToSQL(queryFn func(tx *DB) *DB) string {
	tx := queryFn(db.Session(&Session{DryRun: true, SkipDefaultTransaction: true}))
	stmt := tx.Statement

	return db.Dialector.Explain(stmt.SQL.String(), stmt.Vars...)
}
