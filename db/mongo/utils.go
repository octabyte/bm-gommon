package mongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// InsertOne inserts a single document into a collection
func InsertOne(ctx context.Context, collection *mongo.Collection, document interface{}) (*mongo.InsertOneResult, error) {
	return collection.InsertOne(ctx, document)
}

// InsertMany inserts multiple documents into a collection
func InsertMany(ctx context.Context, collection *mongo.Collection, documents []interface{}) (*mongo.InsertManyResult, error) {
	return collection.InsertMany(ctx, documents)
}

// FindOne retrieves a single document from a collection
func FindOne(ctx context.Context, collection *mongo.Collection, filter interface{}, result interface{}) error {
	return collection.FindOne(ctx, filter).Decode(result)
}

// FindOneWithOptions retrieves a single document with custom options
func FindOneWithOptions(ctx context.Context, collection *mongo.Collection, filter interface{}, result interface{}, opts *options.FindOneOptions) error {
	return collection.FindOne(ctx, filter, opts).Decode(result)
}

// Find retrieves multiple documents from a collection
func Find(ctx context.Context, collection *mongo.Collection, filter interface{}, results interface{}) error {
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	return cursor.All(ctx, results)
}

// FindWithOptions retrieves multiple documents with custom options
func FindWithOptions(ctx context.Context, collection *mongo.Collection, filter interface{}, results interface{}, opts *options.FindOptions) error {
	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	return cursor.All(ctx, results)
}

// UpdateOne updates a single document in a collection
func UpdateOne(ctx context.Context, collection *mongo.Collection, filter interface{}, update interface{}) (*mongo.UpdateResult, error) {
	return collection.UpdateOne(ctx, filter, update)
}

// UpdateMany updates multiple documents in a collection
func UpdateMany(ctx context.Context, collection *mongo.Collection, filter interface{}, update interface{}) (*mongo.UpdateResult, error) {
	return collection.UpdateMany(ctx, filter, update)
}

// ReplaceOne replaces a single document in a collection
func ReplaceOne(ctx context.Context, collection *mongo.Collection, filter interface{}, replacement interface{}) (*mongo.UpdateResult, error) {
	return collection.ReplaceOne(ctx, filter, replacement)
}

// DeleteOne deletes a single document from a collection
func DeleteOne(ctx context.Context, collection *mongo.Collection, filter interface{}) (*mongo.DeleteResult, error) {
	return collection.DeleteOne(ctx, filter)
}

// DeleteMany deletes multiple documents from a collection
func DeleteMany(ctx context.Context, collection *mongo.Collection, filter interface{}) (*mongo.DeleteResult, error) {
	return collection.DeleteMany(ctx, filter)
}

// CountDocuments counts the number of documents in a collection matching the filter
func CountDocuments(ctx context.Context, collection *mongo.Collection, filter interface{}) (int64, error) {
	return collection.CountDocuments(ctx, filter)
}

// Aggregate executes an aggregation pipeline
func Aggregate(ctx context.Context, collection *mongo.Collection, pipeline interface{}, results interface{}) error {
	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	return cursor.All(ctx, results)
}

// FindOneAndUpdate finds a single document and updates it, returning the document
func FindOneAndUpdate(ctx context.Context, collection *mongo.Collection, filter interface{}, update interface{}, result interface{}, returnAfter bool) error {
	opts := options.FindOneAndUpdate()
	if returnAfter {
		opts.SetReturnDocument(options.After)
	} else {
		opts.SetReturnDocument(options.Before)
	}

	return collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(result)
}

// FindOneAndDelete finds a single document and deletes it, returning the deleted document
func FindOneAndDelete(ctx context.Context, collection *mongo.Collection, filter interface{}, result interface{}) error {
	return collection.FindOneAndDelete(ctx, filter).Decode(result)
}

// BulkWrite executes multiple write operations in a single call
func BulkWrite(ctx context.Context, collection *mongo.Collection, operations []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
	return collection.BulkWrite(ctx, operations)
}

// CreateIndex creates an index on a collection
func CreateIndex(ctx context.Context, collection *mongo.Collection, keys bson.D, unique bool) (string, error) {
	indexModel := mongo.IndexModel{
		Keys:    keys,
		Options: options.Index().SetUnique(unique),
	}
	return collection.Indexes().CreateOne(ctx, indexModel)
}

// CreateIndexWithOptions creates an index with custom options
func CreateIndexWithOptions(ctx context.Context, collection *mongo.Collection, indexModel mongo.IndexModel) (string, error) {
	return collection.Indexes().CreateOne(ctx, indexModel)
}

// DropIndex drops an index from a collection
func DropIndex(ctx context.Context, collection *mongo.Collection, name string) error {
	_, err := collection.Indexes().DropOne(ctx, name)
	return err
}

// ListIndexes lists all indexes on a collection
func ListIndexes(ctx context.Context, collection *mongo.Collection) ([]bson.M, error) {
	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var indexes []bson.M
	if err = cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}

	return indexes, nil
}

// Distinct gets distinct values for a field
func Distinct(ctx context.Context, collection *mongo.Collection, fieldName string, filter interface{}) ([]interface{}, error) {
	return collection.Distinct(ctx, fieldName, filter)
}

// WithTransaction executes a function within a transaction
func WithTransaction(ctx context.Context, client *mongo.Client, fn func(sessCtx mongo.SessionContext) error) error {
	session, err := client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		return nil, fn(sessCtx)
	}

	_, err = session.WithTransaction(ctx, callback)
	return err
}

// Exists checks if any document matches the filter
func Exists(ctx context.Context, collection *mongo.Collection, filter interface{}) (bool, error) {
	count, err := collection.CountDocuments(ctx, filter, options.Count().SetLimit(1))
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// FindWithPagination retrieves documents with pagination support
func FindWithPagination(ctx context.Context, collection *mongo.Collection, filter interface{}, results interface{}, page, pageSize int64) error {
	skip := (page - 1) * pageSize
	opts := options.Find().SetSkip(skip).SetLimit(pageSize)

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	return cursor.All(ctx, results)
}

// PaginationResult holds pagination information along with results
type PaginationResult struct {
	Data       interface{} `json:"data"`
	Total      int64       `json:"total"`
	Page       int64       `json:"page"`
	PageSize   int64       `json:"page_size"`
	TotalPages int64       `json:"total_pages"`
}

// FindWithPaginationInfo retrieves documents with complete pagination information
func FindWithPaginationInfo(ctx context.Context, collection *mongo.Collection, filter interface{}, results interface{}, page, pageSize int64) (*PaginationResult, error) {
	// Get total count
	total, err := collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}

	// Get paginated results
	skip := (page - 1) * pageSize
	opts := options.Find().SetSkip(skip).SetLimit(pageSize)

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	if err = cursor.All(ctx, results); err != nil {
		return nil, err
	}

	// Calculate total pages
	totalPages := total / pageSize
	if total%pageSize > 0 {
		totalPages++
	}

	return &PaginationResult{
		Data:       results,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: totalPages,
	}, nil
}

// UpsertOne inserts or updates a document
func UpsertOne(ctx context.Context, collection *mongo.Collection, filter interface{}, update interface{}) (*mongo.UpdateResult, error) {
	opts := options.Update().SetUpsert(true)
	return collection.UpdateOne(ctx, filter, update, opts)
}

// UpdateWithTimestamp updates a document and adds/updates a timestamp field
func UpdateWithTimestamp(ctx context.Context, collection *mongo.Collection, filter interface{}, update bson.M, timestampField string) (*mongo.UpdateResult, error) {
	if update["$set"] == nil {
		update["$set"] = bson.M{}
	}
	update["$set"].(bson.M)[timestampField] = time.Now()

	return collection.UpdateOne(ctx, filter, update)
}

// SoftDelete marks a document as deleted without actually removing it
func SoftDelete(ctx context.Context, collection *mongo.Collection, filter interface{}, deletedAtField string) (*mongo.UpdateResult, error) {
	update := bson.M{
		"$set": bson.M{
			deletedAtField: time.Now(),
		},
	}
	return collection.UpdateOne(ctx, filter, update)
}

// FindActive retrieves documents that are not soft-deleted
func FindActive(ctx context.Context, collection *mongo.Collection, filter bson.M, results interface{}, deletedAtField string) error {
	// Add condition to filter out deleted documents
	if filter == nil {
		filter = bson.M{}
	}
	filter[deletedAtField] = bson.M{"$exists": false}

	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	return cursor.All(ctx, results)
}
