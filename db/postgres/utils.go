package postgres

import (
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Create creates a new record
func Create(ctx context.Context, db *gorm.DB, value interface{}) error {
	return db.WithContext(ctx).Create(value).Error
}

// CreateInBatches creates multiple records in batches
func CreateInBatches(ctx context.Context, db *gorm.DB, value interface{}, batchSize int) error {
	return db.WithContext(ctx).CreateInBatches(value, batchSize).Error
}

// First finds the first record ordered by primary key
func First(ctx context.Context, db *gorm.DB, dest interface{}, conds ...interface{}) error {
	return db.WithContext(ctx).First(dest, conds...).Error
}

// Last finds the last record ordered by primary key
func Last(ctx context.Context, db *gorm.DB, dest interface{}, conds ...interface{}) error {
	return db.WithContext(ctx).Last(dest, conds...).Error
}

// Find finds all records matching given conditions
func Find(ctx context.Context, db *gorm.DB, dest interface{}, conds ...interface{}) error {
	return db.WithContext(ctx).Find(dest, conds...).Error
}

// FindOne finds one record that matches given conditions
func FindOne(ctx context.Context, db *gorm.DB, dest interface{}, conds ...interface{}) error {
	return db.WithContext(ctx).Where(conds[0], conds[1:]...).First(dest).Error
}

// FindWithPagination finds records with pagination
func FindWithPagination(ctx context.Context, db *gorm.DB, dest interface{}, page, pageSize int, conds ...interface{}) error {
	offset := (page - 1) * pageSize
	query := db.WithContext(ctx)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Offset(offset).Limit(pageSize).Find(dest).Error
}

// PaginationResult holds pagination information along with results
type PaginationResult struct {
	Data       interface{} `json:"data"`
	Total      int64       `json:"total"`
	Page       int         `json:"page"`
	PageSize   int         `json:"page_size"`
	TotalPages int         `json:"total_pages"`
}

// FindWithPaginationInfo finds records with complete pagination information
func FindWithPaginationInfo(ctx context.Context, db *gorm.DB, model interface{}, dest interface{}, page, pageSize int, conds ...interface{}) (*PaginationResult, error) {
	query := db.WithContext(ctx).Model(model)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	// Get total count
	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, err
	}

	// Get paginated results
	offset := (page - 1) * pageSize
	if err := query.Offset(offset).Limit(pageSize).Find(dest).Error; err != nil {
		return nil, err
	}

	// Calculate total pages
	totalPages := int(total) / pageSize
	if int(total)%pageSize > 0 {
		totalPages++
	}

	return &PaginationResult{
		Data:       dest,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
	}, nil
}

// Update updates record(s) with given conditions
func Update(ctx context.Context, db *gorm.DB, model interface{}, column string, value interface{}, conds ...interface{}) error {
	query := db.WithContext(ctx).Model(model)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Update(column, value).Error
}

// Updates updates multiple columns
func Updates(ctx context.Context, db *gorm.DB, model interface{}, values interface{}, conds ...interface{}) error {
	query := db.WithContext(ctx).Model(model)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Updates(values).Error
}

// Save saves all fields (insert or update)
func Save(ctx context.Context, db *gorm.DB, value interface{}) error {
	return db.WithContext(ctx).Save(value).Error
}

// Delete deletes record(s) matching given conditions
func Delete(ctx context.Context, db *gorm.DB, model interface{}, conds ...interface{}) error {
	return db.WithContext(ctx).Delete(model, conds...).Error
}

// SoftDelete performs a soft delete (updates deleted_at timestamp)
// Note: Your model must have gorm.DeletedAt field
func SoftDelete(ctx context.Context, db *gorm.DB, model interface{}, conds ...interface{}) error {
	return db.WithContext(ctx).Delete(model, conds...).Error
}

// Unscoped returns a query that includes soft-deleted records
func Unscoped(db *gorm.DB) *gorm.DB {
	return db.Unscoped()
}

// Count counts records matching given conditions
func Count(ctx context.Context, db *gorm.DB, model interface{}, count *int64, conds ...interface{}) error {
	query := db.WithContext(ctx).Model(model)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Count(count).Error
}

// Exists checks if any record matches the given conditions
func Exists(ctx context.Context, db *gorm.DB, model interface{}, conds ...interface{}) (bool, error) {
	var count int64
	query := db.WithContext(ctx).Model(model)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	if err := query.Limit(1).Count(&count).Error; err != nil {
		return false, err
	}

	return count > 0, nil
}

// FirstOrCreate finds the first matched record or creates a new one
func FirstOrCreate(ctx context.Context, db *gorm.DB, dest interface{}, conds ...interface{}) error {
	return db.WithContext(ctx).FirstOrCreate(dest, conds...).Error
}

// FirstOrInit finds the first matched record or initializes a new one
func FirstOrInit(ctx context.Context, db *gorm.DB, dest interface{}, conds ...interface{}) error {
	return db.WithContext(ctx).FirstOrInit(dest, conds...).Error
}

// Upsert performs an insert or update (using ON CONFLICT)
func Upsert(ctx context.Context, db *gorm.DB, value interface{}, conflictColumns []string, updateColumns []string) error {
	return db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   columnsToClause(conflictColumns),
		DoUpdates: clause.AssignmentColumns(updateColumns),
	}).Create(value).Error
}

// Transaction executes a function within a database transaction
func Transaction(ctx context.Context, db *gorm.DB, fn func(*gorm.DB) error) error {
	return db.WithContext(ctx).Transaction(fn)
}

// Raw executes raw SQL query
func Raw(ctx context.Context, db *gorm.DB, dest interface{}, sql string, values ...interface{}) error {
	return db.WithContext(ctx).Raw(sql, values...).Scan(dest).Error
}

// Exec executes raw SQL command (INSERT, UPDATE, DELETE)
func Exec(ctx context.Context, db *gorm.DB, sql string, values ...interface{}) error {
	return db.WithContext(ctx).Exec(sql, values...).Error
}

// Pluck retrieves single column as slice
func Pluck(ctx context.Context, db *gorm.DB, model interface{}, column string, dest interface{}, conds ...interface{}) error {
	query := db.WithContext(ctx).Model(model)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Pluck(column, dest).Error
}

// Distinct finds distinct values
func Distinct(ctx context.Context, db *gorm.DB, model interface{}, dest interface{}, column string, conds ...interface{}) error {
	query := db.WithContext(ctx).Model(model).Distinct(column)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Find(dest).Error
}

// Select specifies fields to retrieve
func Select(ctx context.Context, db *gorm.DB, dest interface{}, fields []string, conds ...interface{}) error {
	query := db.WithContext(ctx).Select(fields)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Find(dest).Error
}

// Joins performs a join query
func Joins(ctx context.Context, db *gorm.DB, dest interface{}, joinQuery string, conds ...interface{}) error {
	query := db.WithContext(ctx).Joins(joinQuery)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Find(dest).Error
}

// Group performs a GROUP BY query
func Group(ctx context.Context, db *gorm.DB, dest interface{}, groupBy string, conds ...interface{}) error {
	query := db.WithContext(ctx).Group(groupBy)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Find(dest).Error
}

// Order specifies order when retrieving records
func Order(ctx context.Context, db *gorm.DB, dest interface{}, orderBy string, conds ...interface{}) error {
	query := db.WithContext(ctx).Order(orderBy)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Find(dest).Error
}

// FindInBatches finds records in batches
func FindInBatches(ctx context.Context, db *gorm.DB, dest interface{}, batchSize int, fn func(*gorm.DB, int) error) error {
	return db.WithContext(ctx).FindInBatches(dest, batchSize, fn).Error
}

// AutoMigrate runs auto migration for given models
func AutoMigrate(db *gorm.DB, models ...interface{}) error {
	return db.AutoMigrate(models...)
}

// CreateTable creates table for given model
func CreateTable(db *gorm.DB, model interface{}) error {
	return db.Migrator().CreateTable(model)
}

// DropTable drops table for given model
func DropTable(db *gorm.DB, model interface{}) error {
	return db.Migrator().DropTable(model)
}

// HasTable checks if table exists
func HasTable(db *gorm.DB, model interface{}) bool {
	return db.Migrator().HasTable(model)
}

// AddColumn adds a column to table
func AddColumn(db *gorm.DB, model interface{}, column string) error {
	return db.Migrator().AddColumn(model, column)
}

// DropColumn drops a column from table
func DropColumn(db *gorm.DB, model interface{}, column string) error {
	return db.Migrator().DropColumn(model, column)
}

// CreateIndex creates an index
func CreateIndex(db *gorm.DB, model interface{}, indexName string) error {
	return db.Migrator().CreateIndex(model, indexName)
}

// DropIndex drops an index
func DropIndex(db *gorm.DB, model interface{}, indexName string) error {
	return db.Migrator().DropIndex(model, indexName)
}

// Helper function to convert string slice to clause columns
func columnsToClause(columns []string) []clause.Column {
	result := make([]clause.Column, len(columns))
	for i, col := range columns {
		result[i] = clause.Column{Name: col}
	}
	return result
}

// Preload preloads associations
func Preload(ctx context.Context, db *gorm.DB, dest interface{}, preloadAssociations []string, conds ...interface{}) error {
	query := db.WithContext(ctx)

	for _, assoc := range preloadAssociations {
		query = query.Preload(assoc)
	}

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.Find(dest).Error
}

// UpdateColumn updates a single column (skips hooks and timestamp updates)
func UpdateColumn(ctx context.Context, db *gorm.DB, model interface{}, column string, value interface{}, conds ...interface{}) error {
	query := db.WithContext(ctx).Model(model)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.UpdateColumn(column, value).Error
}

// UpdateColumns updates multiple columns (skips hooks and timestamp updates)
func UpdateColumns(ctx context.Context, db *gorm.DB, model interface{}, values interface{}, conds ...interface{}) error {
	query := db.WithContext(ctx).Model(model)

	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}

	return query.UpdateColumns(values).Error
}
