package callbacks

import (
	"gorm.io/gorm"
)

func BeginTransaction(db *gorm.DB) {
	// 如果没有配置跳过事务，并且没错误
	if !db.Config.SkipDefaultTransaction && db.Error == nil {
		if tx := db.Begin(); tx.Error == nil { // 开始一个事务
			db.Statement.ConnPool = tx.Statement.ConnPool
			db.InstanceSet("gorm:started_transaction", true)
		} else if tx.Error == gorm.ErrInvalidTransaction {
			tx.Error = nil
		} else {
			db.Error = tx.Error
		}
	}
}

func CommitOrRollbackTransaction(db *gorm.DB) {
	if !db.Config.SkipDefaultTransaction {
		if _, ok := db.InstanceGet("gorm:started_transaction"); ok { // 如果已经开启了事务
			if db.Error != nil {
				db.Rollback() // 出错回滚
			} else {
				db.Commit() // 成功提交
			}

			db.Statement.ConnPool = db.ConnPool
		}
	}
}
