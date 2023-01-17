package database

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/accuknox/kmux/config"
	"github.com/accuknox/kmux/vault"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type tableMap map[string]any

// MongoHandle wraps core MongoDB database
type MongoHandle struct {
	db *mongo.Database
}

// NewMongoSink returns a new MongoDB sink
func NewMongoSink() *MongoHandle {
	return &MongoHandle{}
}

// NewMongoSource returns a new MongoDB source
func NewMongoSource() *MongoHandle {
	return &MongoHandle{}
}

// Connect is used to connect with the mongoDB database specified in kmux configuration
func (m *MongoHandle) Connect() error {

	var err error
	conf := config.Database
	username := conf.Username
	password := conf.Password

	if conf.Vault != nil {

		if conf.Vault.Key.Password == "" || conf.Vault.Key.Username == "" || conf.Vault.SecretPath == "" {
			return fmt.Errorf("database.vault is not configured with all mandatory fields in config file")
		}
		username, password, err = getCredentialsFromVault(conf)
		if err != nil {
			return fmt.Errorf("Failed to get Database credentials from Vault, %v", err)
		}
	}

	url := getMongoURL(conf.Server, username, password, conf.ConnParams)

	log.Info().Msgf("Connecting to mongoDB server - %s", url.Redacted())

	conn, err := mongo.Connect(context.Background(), options.Client().ApplyURI(url.String()))
	if err != nil {
		log.Error().Msgf("Failed to connect with database. %s", err)
		return err
	}

	err = conn.Ping(context.TODO(), nil)
	if err != nil {
		log.Error().Msgf("Failed to ping the database. %s", err)
		return err
	}

	m.db = conn.Database(conf.Name)
	return nil
}

func getCredentialsFromVault(conf config.DatabaseConfig) (user, password string, err error) {
	vault, err := vault.NewVault(conf.Vault.SecretPath)
	if err != nil {
		return "", "", fmt.Errorf("Failed to create new Vault instance. %v", err)
	}
	usernameVault, err := vault.GetSecret(conf.Vault.Key.Username)
	if err != nil {
		return "", "", fmt.Errorf("Failed to fetch database username from Vault. %v", err)
	}
	passwordVault, err := vault.GetSecret(conf.Vault.Key.Password)
	if err != nil {
		return "", "", fmt.Errorf("Failed to fetch database password from Vault. %v", err)
	}
	return usernameVault.(string), passwordVault.(string), nil
}

// Disconnect terminates the connection with mongoDB database
func (m *MongoHandle) Disconnect() {
	err := m.db.Client().Disconnect(context.Background())
	if err != nil {
		log.Error().Msgf("Failed to terminate the connection with database. %s", err)
	}
}

// Upsert performs either insert or update in mongoDB collection `collName`
// Upsert uses `record.Where` and `record.Args` to checks for any existing
// documents. If found, `record.Row` will be used to update such documents
// in the collection. If not found, then `record.Row` will be inserted as
// a new document in the collection.
func (m *MongoHandle) Upsert(collName string, record Record) error {
	if collName == "" {
		return fmt.Errorf("Collection name should not be empty")
	}

	collection := m.db.Collection(collName)

	filter, err := convertSQLQueryToBsonD(record.Where, record.Args)
	if err != nil {
		log.Error().Msgf("Failed to parse where clause. %s", err)
		return err
	}

	mapRef := isMapRef(record.Row)
	structRef := isStructRef(record.Row)

	if !mapRef && !structRef {
		return fmt.Errorf("Passed value of type %T. Expected reference to a struct or map", record.Row)
	}

	var row any
	if structRef {
		row = getRetaggedStruct(record.Row, kmuxToBsonTagger{})
	} else {
		row = record.Row
	}

	insert := false
	if len(filter) == 0 {
		insert = true
	}

	if !insert {
		updateDoc := bson.D{{"$set", row}}
		timeBeforeQuery := time.Now()
		updateRes, err := collection.UpdateMany(context.Background(), filter, updateDoc, nil)
		if err != nil {
			log.Error().Msgf("Failed to update the record. %s", err)
			return err
		}
		if updateRes.MatchedCount == 0 {
			insert = true
		}
		log.Info().Msgf("[%dms] [rows: %d] collection.UpdateMany(filter: %v, update: %v)",
			time.Since(timeBeforeQuery).Milliseconds(), updateRes.MatchedCount, filter, updateDoc)
	}

	if insert {
		timeBeforeQuery := time.Now()
		_, err := collection.InsertOne(context.Background(), row, nil)
		if err != nil {
			log.Error().Msgf("Failed to insert the record. %s", err)
			return err
		}
		log.Info().Msgf("[%dms] [rows: 1] collection.InsertOne(document: %v)",
			time.Since(timeBeforeQuery).Milliseconds(), row)
	}

	return nil
}

// Get uses `query.Where` and `query.Args` to query the mongoDB collection `collName`.
// Get stores the results in `query.Rows`. Returns `error` if error occurred.
func (m *MongoHandle) Get(collName string, query Query) error {
	if collName == "" {
		return fmt.Errorf("Collection name should not be empty")
	}

	if len(query.GroupBy) != 0 {
		// Queries with GroupBy needs to handled using mongoDB aggregation
		// pipeline API which is different from collection.Find() used below.
		return m.processQueryWithGroupBy(collName, query)
	}

	mapSliceRef := isMapSliceRef(query.Rows)
	structSliceRef := isStructSliceRef(query.Rows)

	if !mapSliceRef && !structSliceRef {
		return fmt.Errorf("Passed value of type %T. Expected reference to a []struct or []map", query.Rows)
	}

	collection := m.db.Collection(collName)

	filter, err := convertSQLQueryToBsonD(query.Where, query.Args)
	if err != nil {
		log.Error().Msgf("Failed to parse where clause. %s", err)
		return err
	}

	opt := &options.FindOptions{
		Limit: &query.Limit,
		Skip:  &query.Offset,
		Sort:  convertOrderByColumnsToBsonD(query.OrderBy),
	}

	timeBeforeQuery := time.Now()

	cursor, err := collection.Find(context.Background(), filter, opt)
	if err != nil {
		log.Error().Msgf("Failed to query the database. %s", err)
		return err
	}

	var rows any
	if structSliceRef {
		rows, err = makeRetaggedSlice(query.Rows, kmuxToBsonTagger{})
		if err != nil {
			log.Error().Msgf("Failed to process struct tags. %s", err)
			return err
		}
	} else {
		rows = query.Rows
	}

	err = cursor.All(context.Background(), rows)
	if err != nil {
		log.Error().Msgf("Failed to retrieve the records. %s", err)
		return err
	}

	rowLen := reflect.Indirect(reflect.ValueOf(rows)).Len()
	log.Info().Msgf("[%dms] [rows: %d] collection.Find(filter: %v, opts: %v)",
		time.Since(timeBeforeQuery).Milliseconds(), rowLen, filter, opt)
	if structSliceRef {
		err = copySliceElements(query.Rows, rows)
		if err != nil {
			log.Error().Msgf("Failed to copy retrieved records. %s", err)
			return err
		}
	}

	return nil
}

// Count returns the total count of records matching `query.Where` and `query.Args`.
// Returns `error` if error occurred.
func (m *MongoHandle) Count(collName string, query Query) (int32, error) {
	noCount := int32(0)

	if collName == "" {
		return noCount, fmt.Errorf("Collection name should not be empty")
	}

	collection := m.db.Collection(collName)

	filter, err := convertSQLQueryToBsonD(query.Where, query.Args)
	if err != nil {
		log.Error().Msgf("Failed to parse where clause. %s", err)
		return noCount, err
	}

	groupBy := convertGroupByColumnsToBsonD(query.GroupBy)

	pipeline := mongo.Pipeline{}
	if len(filter) != 0 {
		pipeline = append(pipeline, bson.D{{"$match", filter}})
	}
	if len(groupBy) != 0 {
		pipeline = append(pipeline, bson.D{{"$group", bson.D{{"_id", groupBy}, {"count", bson.M{"$sum": 1}}}}})
	} else {
		pipeline = append(pipeline, bson.D{{"$count", "count"}})
	}

	timeBeforeQuery := time.Now()

	cursor, err := collection.Aggregate(context.Background(), pipeline, nil)
	if err != nil {
		log.Error().Msgf("Failed to query the database. %s", err)
		return noCount, err
	}

	commonErrMsg := "Failed to retrieve the total count of records"

	rows := []tableMap{}
	err = cursor.All(context.Background(), &rows)
	if err != nil {
		log.Error().Msgf("%s. %s", commonErrMsg, err)
		return noCount, err
	}

	if len(rows) == 0 {
		// This will be true when `$match` pipeline returns zero records
		return 0, nil
	}

	rowCount := int32(0)
	if query.GroupBy != nil {
		if len(groupBy) != 0 {
			for i, r := range rows {
				m := r["_id"].(tableMap)
				for k, v := range m {
					newK := strings.ReplaceAll(k, "->", ".")
					delete(m, k)
					m[newK] = v
				}
				delete(rows[i], "_id")
				rows[i]["group"] = m
				rows[i]["count"] = r["count"]
			}
		}
		rowCount = int32(len(rows))
		if query.Rows != nil {
			err = copySliceElements(query.Rows, &rows)
			if err != nil {
				log.Error().Msgf("Failed to copy retrieved records. %s", err)
				return 0, err
			}
		}
	} else {
		ok := true
		rowCount, ok = rows[0]["count"].(int32)
		if !ok {
			log.Error().Msgf(commonErrMsg)
			return noCount, fmt.Errorf(commonErrMsg)
		}
	}

	log.Info().Msgf("[%dms] [rows: %d] collection.Aggregate(pipeline: %v, opts: nil)",
		time.Since(timeBeforeQuery).Milliseconds(), len(rows), filter, pipeline)

	return rowCount, nil
}

func getMongoURL(server, username, password string, params []string) url.URL {
	// Connection URL Format
	// mongodb://username:password@host:port/?param1=value1&param2=value2

	var userInfo *url.Userinfo
	if username != "" {
		userInfo = url.UserPassword(username, password)
	}

	return url.URL{
		Scheme:   "mongodb",
		User:     userInfo,
		Host:     server,
		Path:     "/",
		RawQuery: strings.Join(params, "&"),
	}
}

func isMapRef(val any) bool {
	v := reflect.ValueOf(val)
	if v.Kind() == reflect.Pointer && v.Elem().Kind() == reflect.Map {
		return true
	}

	return false
}

func isStructRef(val any) bool {
	v := reflect.ValueOf(val)
	if v.Kind() == reflect.Pointer && v.Elem().Kind() == reflect.Struct {
		return true
	}

	return false
}

func isSliceRef(v reflect.Value) bool {
	if v.Kind() == reflect.Pointer && v.Elem().Kind() == reflect.Slice {
		return true
	}
	return false
}

func isMapSliceRef(val any) bool {
	v := reflect.ValueOf(val)
	if !isSliceRef(v) {
		return false
	}

	elemType := v.Elem().Type().Elem()
	if elemType.Kind() != reflect.Map {
		return false
	}

	return true
}

func isStructSliceRef(val any) bool {
	v := reflect.ValueOf(val)
	if !isSliceRef(v) {
		return false
	}

	elemType := v.Elem().Type().Elem()
	if elemType.Kind() != reflect.Struct {
		return false
	}

	return true
}

func (m *MongoHandle) processQueryWithGroupBy(collName string, query Query) error {
	mapSliceRef := isMapSliceRef(query.Rows)

	if !mapSliceRef {
		return fmt.Errorf("Passed value of type %T. Expected reference to a []map[string]any", query.Rows)
	}

	collection := m.db.Collection(collName)

	filter, err := convertSQLQueryToBsonD(query.Where, query.Args)
	if err != nil {
		log.Error().Msgf("Failed to parse where clause. %s", err)
		return err
	}

	groupBy := convertGroupByColumnsToBsonD(query.GroupBy)
	orderBy := convertOrderByColumnsToBsonD(query.OrderBy)

	pipeline := mongo.Pipeline{}
	if len(filter) != 0 {
		pipeline = append(pipeline, bson.D{{"$match", filter}})
	}
	pipeline = append(pipeline, bson.D{{"$group", bson.D{{"_id", groupBy}}}})
	if len(query.OrderBy) != 0 {
		pipeline = append(pipeline, bson.D{{"$sort", orderBy}})
	}
	if query.Offset != 0 {
		pipeline = append(pipeline, bson.D{{"$skip", query.Offset}})
	}
	if query.Limit != 0 {
		pipeline = append(pipeline, bson.D{{"$limit", query.Limit}})
	}

	timeBeforeQuery := time.Now()

	cursor, err := collection.Aggregate(context.Background(), pipeline, nil)
	if err != nil {
		log.Error().Msgf("Failed to query the database. %s", err)
		return err
	}

	rows := []tableMap{}
	err = cursor.All(context.Background(), &rows)
	if err != nil {
		log.Error().Msgf("Failed to retrieve the records. %s", err)
		return err
	}

	log.Info().Msgf("[%dms] [rows: %d] collection.Aggregate(pipeline: %v, opts: nil)",
		time.Since(timeBeforeQuery).Milliseconds(), len(rows), pipeline)

	for i, r := range rows {
		m := r["_id"].(tableMap)
		for k, v := range m {
			newK := strings.ReplaceAll(k, "->", ".")
			delete(m, k)
			m[newK] = v
		}
		rows[i] = m
	}

	err = copySliceElements(query.Rows, &rows)
	if err != nil {
		log.Error().Msgf("Failed to copy retrieved records. %s", err)
		return err
	}

	return nil
}
