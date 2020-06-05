package tests_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
	. "gorm.io/gorm/utils/tests"
)

type Product struct {
	gorm.Model
	Name                  string
	Code                  string
	Price                 float64
	AfterFindCallTimes    int64
	BeforeCreateCallTimes int64
	AfterCreateCallTimes  int64
	BeforeUpdateCallTimes int64
	AfterUpdateCallTimes  int64
	BeforeSaveCallTimes   int64
	AfterSaveCallTimes    int64
	BeforeDeleteCallTimes int64
	AfterDeleteCallTimes  int64
}

func (s *Product) BeforeCreate(tx *gorm.DB) (err error) {
	if s.Code == "Invalid" {
		err = errors.New("invalid product")
	}
	s.BeforeCreateCallTimes = s.BeforeCreateCallTimes + 1
	return
}

func (s *Product) BeforeUpdate(tx *gorm.DB) (err error) {
	if s.Code == "dont_update" {
		err = errors.New("can't update")
	}
	s.BeforeUpdateCallTimes = s.BeforeUpdateCallTimes + 1
	return
}

func (s *Product) BeforeSave(tx *gorm.DB) (err error) {
	if s.Code == "dont_save" {
		err = errors.New("can't save")
	}
	s.BeforeSaveCallTimes = s.BeforeSaveCallTimes + 1
	return
}

func (s *Product) AfterFind(tx *gorm.DB) (err error) {
	s.AfterFindCallTimes = s.AfterFindCallTimes + 1
	return
}

func (s *Product) AfterCreate(tx *gorm.DB) (err error) {
	return tx.Model(s).UpdateColumn("AfterCreateCallTimes", s.AfterCreateCallTimes+1).Error
}

func (s *Product) AfterUpdate(tx *gorm.DB) (err error) {
	s.AfterUpdateCallTimes = s.AfterUpdateCallTimes + 1
	return
}

func (s *Product) AfterSave(tx *gorm.DB) (err error) {
	if s.Code == "after_save_error" {
		err = errors.New("can't save")
	}
	s.AfterSaveCallTimes = s.AfterSaveCallTimes + 1
	return
}

func (s *Product) BeforeDelete(tx *gorm.DB) (err error) {
	if s.Code == "dont_delete" {
		err = errors.New("can't delete")
	}
	s.BeforeDeleteCallTimes = s.BeforeDeleteCallTimes + 1
	return
}

func (s *Product) AfterDelete(tx *gorm.DB) (err error) {
	if s.Code == "after_delete_error" {
		err = errors.New("can't delete")
	}
	s.AfterDeleteCallTimes = s.AfterDeleteCallTimes + 1
	return
}

func (s *Product) GetCallTimes() []int64 {
	return []int64{s.BeforeCreateCallTimes, s.BeforeSaveCallTimes, s.BeforeUpdateCallTimes, s.AfterCreateCallTimes, s.AfterSaveCallTimes, s.AfterUpdateCallTimes, s.BeforeDeleteCallTimes, s.AfterDeleteCallTimes, s.AfterFindCallTimes}
}

func TestRunCallbacks(t *testing.T) {
	DB.Migrator().DropTable(&Product{})
	DB.AutoMigrate(&Product{})

	p := Product{Code: "unique_code", Price: 100}
	DB.Save(&p)

	if !reflect.DeepEqual(p.GetCallTimes(), []int64{1, 1, 0, 1, 1, 0, 0, 0, 0}) {
		t.Fatalf("Callbacks should be invoked successfully, %v", p.GetCallTimes())
	}

	DB.Where("Code = ?", "unique_code").First(&p)
	if !reflect.DeepEqual(p.GetCallTimes(), []int64{1, 1, 0, 1, 0, 0, 0, 0, 1}) {
		t.Fatalf("After callbacks values are not saved, %v", p.GetCallTimes())
	}

	p.Price = 200
	DB.Save(&p)
	if !reflect.DeepEqual(p.GetCallTimes(), []int64{1, 2, 1, 1, 1, 1, 0, 0, 1}) {
		t.Fatalf("After update callbacks should be invoked successfully, %v", p.GetCallTimes())
	}

	var products []Product
	DB.Find(&products, "code = ?", "unique_code")
	if products[0].AfterFindCallTimes != 2 {
		t.Fatalf("AfterFind callbacks should work with slice, called %v", products[0].AfterFindCallTimes)
	}

	DB.Where("Code = ?", "unique_code").First(&p)
	if !reflect.DeepEqual(p.GetCallTimes(), []int64{1, 2, 1, 1, 0, 0, 0, 0, 2}) {
		t.Fatalf("After update callbacks values are not saved, %v", p.GetCallTimes())
	}

	DB.Delete(&p)
	if !reflect.DeepEqual(p.GetCallTimes(), []int64{1, 2, 1, 1, 0, 0, 1, 1, 2}) {
		t.Fatalf("After delete callbacks should be invoked successfully, %v", p.GetCallTimes())
	}

	if DB.Where("Code = ?", "unique_code").First(&p).Error == nil {
		t.Fatalf("Can't find a deleted record")
	}
}

func TestCallbacksWithErrors(t *testing.T) {
	DB.Migrator().DropTable(&Product{})
	DB.AutoMigrate(&Product{})

	p := Product{Code: "Invalid", Price: 100}
	if DB.Save(&p).Error == nil {
		t.Fatalf("An error from before create callbacks happened when create with invalid value")
	}

	if DB.Where("code = ?", "Invalid").First(&Product{}).Error == nil {
		t.Fatalf("Should not save record that have errors")
	}

	if DB.Save(&Product{Code: "dont_save", Price: 100}).Error == nil {
		t.Fatalf("An error from after create callbacks happened when create with invalid value")
	}

	p2 := Product{Code: "update_callback", Price: 100}
	DB.Save(&p2)

	p2.Code = "dont_update"
	if DB.Save(&p2).Error == nil {
		t.Fatalf("An error from before update callbacks happened when update with invalid value")
	}

	if DB.Where("code = ?", "update_callback").First(&Product{}).Error != nil {
		t.Fatalf("Record Should not be updated due to errors happened in before update callback")
	}

	if DB.Where("code = ?", "dont_update").First(&Product{}).Error == nil {
		t.Fatalf("Record Should not be updated due to errors happened in before update callback")
	}

	p2.Code = "dont_save"
	if DB.Save(&p2).Error == nil {
		t.Fatalf("An error from before save callbacks happened when update with invalid value")
	}

	p3 := Product{Code: "dont_delete", Price: 100}
	DB.Save(&p3)
	if DB.Delete(&p3).Error == nil {
		t.Fatalf("An error from before delete callbacks happened when delete")
	}

	if DB.Where("Code = ?", "dont_delete").First(&p3).Error != nil {
		t.Fatalf("An error from before delete callbacks happened")
	}

	p4 := Product{Code: "after_save_error", Price: 100}
	DB.Save(&p4)
	if err := DB.First(&Product{}, "code = ?", "after_save_error").Error; err == nil {
		t.Fatalf("Record should be reverted if get an error in after save callback")
	}

	p5 := Product{Code: "after_delete_error", Price: 100}
	DB.Save(&p5)
	if err := DB.First(&Product{}, "code = ?", "after_delete_error").Error; err != nil {
		t.Fatalf("Record should be found")
	}

	DB.Delete(&p5)
	if err := DB.First(&Product{}, "code = ?", "after_delete_error").Error; err != nil {
		t.Fatalf("Record shouldn't be deleted because of an error happened in after delete callback")
	}
}

type Product2 struct {
	gorm.Model
	Name  string
	Code  string
	Price int64
	Owner string
}

func (s Product2) BeforeCreate(tx *gorm.DB) (err error) {
	if !strings.HasSuffix(s.Name, "_clone") {
		newProduft := s
		newProduft.Price *= 2
		newProduft.Name += "_clone"
		err = tx.Create(&newProduft).Error
	}

	if s.Name == "Invalid" {
		return errors.New("invalid")
	}

	return nil
}

func (s *Product2) BeforeUpdate(tx *gorm.DB) (err error) {
	tx.Statement.Where("owner != ?", "admin")
	return
}

func TestUseDBInHooks(t *testing.T) {
	DB.Migrator().DropTable(&Product2{})
	DB.AutoMigrate(&Product2{})

	product := Product2{Name: "Invalid", Price: 100}

	if err := DB.Create(&product).Error; err == nil {
		t.Fatalf("should returns error %v when creating product, but got nil", err)
	}

	product2 := Product2{Name: "Nice", Price: 100}

	if err := DB.Create(&product2).Error; err != nil {
		t.Fatalf("Failed to create product, got error: %v", err)
	}

	var result Product2
	if err := DB.First(&result, "name = ?", "Nice").Error; err != nil {
		t.Fatalf("Failed to query product, got error: %v", err)
	}

	var resultClone Product2
	if err := DB.First(&resultClone, "name = ?", "Nice_clone").Error; err != nil {
		t.Fatalf("Failed to find cloned product, got error: %v", err)
	}

	result.Price *= 2
	result.Name += "_clone"
	AssertObjEqual(t, result, resultClone, "Price", "Name")

	DB.Model(&result).Update("Price", 500)
	var result2 Product2
	DB.First(&result2, "name = ?", "Nice")

	if result2.Price != 500 {
		t.Errorf("Failed to update product's price, expects: %v, got %v", 500, result2.Price)
	}

	product3 := Product2{Name: "Nice2", Price: 600, Owner: "admin"}
	if err := DB.Create(&product3).Error; err != nil {
		t.Fatalf("Failed to create product, got error: %v", err)
	}

	var result3 Product2
	if err := DB.First(&result3, "name = ?", "Nice2").Error; err != nil {
		t.Fatalf("Failed to query product, got error: %v", err)
	}

	DB.Model(&result3).Update("Price", 800)
	var result4 Product2
	DB.First(&result4, "name = ?", "Nice2")

	if result4.Price != 600 {
		t.Errorf("Admin product's price should not be changed, expects: %v, got %v", 600, result4.Price)
	}
}
