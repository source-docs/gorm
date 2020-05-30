package mssql

import (
	"fmt"

	"github.com/jinzhu/gorm"
	"github.com/jinzhu/gorm/clause"
	"github.com/jinzhu/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.tables WHERE table_name = ? AND table_catalog = ?",
			stmt.Table, m.CurrentDatabase(),
		).Row().Scan(&count)
	})
	return count > 0
}

func (m Migrator) HasColumn(value interface{}, field string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		currentDatabase := m.DB.Migrator().CurrentDatabase()
		name := field
		if field := stmt.Schema.LookUpField(field); field != nil {
			name = field.DBName
		}

		return m.DB.Raw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.columns WHERE table_catalog = ? AND table_name = ? AND column_name = ?",
			currentDatabase, stmt.Table, name,
		).Row().Scan(&count)
	})

	return count > 0
}

func (m Migrator) HasIndex(value interface{}, name string) bool {
	var count int
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if idx := stmt.Schema.LookIndex(name); idx != nil {
			name = idx.Name
		}

		return m.DB.Raw(
			"SELECT count(*) FROM sys.indexes WHERE name=? AND object_id=OBJECT_ID(?)",
			name, stmt.Table,
		).Row().Scan(&count)
	})
	return count > 0
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {

		return m.DB.Exec(
			"sp_rename @objname = ?, @newname = ?, @objtype = 'INDEX';",
			fmt.Sprintf("%s.%s", stmt.Table, oldName), clause.Column{Name: newName},
		).Error
	})
}

func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			`SELECT count(*) FROM sys.foreign_keys as F inner join sys.tables as T on F.parent_object_id=T.object_id inner join information_schema.tables as I on I.TABLE_NAME = T.name WHERE F.name = ?  AND T.Name = ? AND I.TABLE_CATALOG = ?;`,
			name, stmt.Table, m.CurrentDatabase(),
		).Row().Scan(&count)
	})
	return count > 0
}

func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw("SELECT DB_NAME() AS [Current Database]").Row().Scan(&name)
	return
}
