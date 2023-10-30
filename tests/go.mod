module gorm.io/gorm/tests

go 1.16

require (
	github.com/go-sql-driver/mysql v1.7.1 // indirect
	github.com/google/uuid v1.3.1
	github.com/jinzhu/now v1.1.5
	github.com/lib/pq v1.10.9
	github.com/microsoft/go-mssqldb v1.6.0 // indirect
	gorm.io/driver/mysql v1.5.1
	gorm.io/driver/postgres v1.5.4
	gorm.io/driver/sqlite v1.5.3
	gorm.io/driver/sqlserver v1.5.1
	gorm.io/gorm v1.25.5
)

replace gorm.io/gorm => ../
