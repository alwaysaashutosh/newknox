package database

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/fatih/structtag"
	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/exp/slices"
	parser "vitess.io/vitess/go/vt/sqlparser"
)

func convertGroupByColumnsToBsonD(columns []string) bson.D {
	res := bson.D{}
	for _, column := range columns {
		newCol := strings.ReplaceAll(column, ".", "->")
		selector := "$" + column
		res = append(res, bson.E{newCol, selector})
	}
	return res
}

func convertOrderByColumnsToBsonD(orderBy []OrderByColumn) bson.D {
	res := bson.D{}
	for _, col := range orderBy {
		val := 1
		if col.Desc {
			val = -1
		}
		res = append(res, bson.E{col.Name, val})
	}
	return res
}

func convertSQLQueryToBsonD(whereQuery string, args []any) (bson.D, error) {
	if whereQuery == "" {
		return bson.D{}, nil
	}

	wcArr := strings.Split(whereQuery, "?")

	query := ""
	for i, wc := range wcArr {
		if i >= len(args) {
			query += wc
			continue
		}

		if str, ok := args[i].(string); ok {
			query += fmt.Sprintf("%s'%s'", wc, str)
		} else {
			query += fmt.Sprintf("%s%v", wc, args[i])
		}
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	return convertExprToBsonD(expr), nil
}

func convertExprToBsonD(expr parser.Expr) bson.D {
	switch expr.(type) {
	case *parser.ComparisonExpr:
		cmpExpr := expr.(*parser.ComparisonExpr)
		return convertCmpExprToBsonD(cmpExpr)
	case *parser.AndExpr:
		andExpr := expr.(*parser.AndExpr)

		var leftBsonD, rightBsonD bson.D
		leftBsonD = convertExprToBsonD(andExpr.Left)
		rightBsonD = convertExprToBsonD(andExpr.Right)

		if leftBsonD != nil && rightBsonD != nil {
			return bson.D{{"$and", bson.A{leftBsonD, rightBsonD}}}
		}
	case *parser.OrExpr:
		orExpr := expr.(*parser.OrExpr)

		var leftBsonD, rightBsonD bson.D
		leftBsonD = convertExprToBsonD(orExpr.Left)
		rightBsonD = convertExprToBsonD(orExpr.Right)

		if leftBsonD != nil && rightBsonD != nil {
			return bson.D{{"$or", bson.A{leftBsonD, rightBsonD}}}
		}
	}
	return nil
}

func convertCmpExprToBsonD(expr *parser.ComparisonExpr) bson.D {
	Op := ""
	switch expr.Operator {
	case parser.EqualOp:
		Op = "$eq"
	case parser.NotEqualOp:
		Op = "$ne"
	case parser.GreaterThanOp:
		Op = "$gt"
	case parser.LessThanOp:
		Op = "$lt"
	case parser.GreaterEqualOp:
		Op = "$gte"
	case parser.LessEqualOp:
		Op = "$lte"
	}

	if Op == "" {
		return nil
	}

	fieldName := parser.String(expr.Left)

	// 1) sqlparser library has a set of SQL keywords defined.
	//    https://github.com/vitessio/vitess/blob/4d94e3f0beb3e88be6cb3d13d71f0d4b4e8155fb/go/vt/sqlparser/sql.y#L7182
	// 2) If the column name matches one of the keywords, the
	//    column name gets wrapped within backticks after parsing.
	//    Example, "time" --> "`time`"
	// 3) Remove the backticks before adding the name to bson.D filter.
	fieldName = strings.ReplaceAll(fieldName, "`", "")

	value := getValueFromCmpExprRight(expr.Right)
	if value == nil {
		return nil
	}

	return bson.D{{fieldName, bson.D{{"$exists", true}, {Op, value}}}}
}

func getValueFromCmpExprRight(expr parser.Expr) interface{} {
	var value interface{}

	switch expr.(type) {
	case parser.BoolVal:
		value = expr.(parser.BoolVal)
	case *parser.Literal:
		literal := expr.(*parser.Literal)
		value = getValueFromLiteral(literal)
	}

	return value
}

func getValueFromLiteral(literal *parser.Literal) interface{} {
	var err error
	var value interface{}
	switch literal.Type {
	case parser.StrVal:
		value = literal.Val
	case parser.HexVal:
		value, err = hex.DecodeString(literal.Val)
		if err == nil {
			value = string(value.([]byte))
		}
	case parser.IntVal, parser.HexNum:
		value, err = strconv.ParseInt(literal.Val, 0, 64)
	case parser.DecimalVal, parser.FloatVal:
		value, err = strconv.ParseFloat(literal.Val, 64)
	}

	if err != nil {
		return nil
	}
	return value
}

func sliceToBsonA(slice any, structTag string, omitEmpty bool) bson.A {
	res := bson.A{}

	v := reflect.ValueOf(slice)

	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil
	}

	for i := 0; i < v.Len(); i++ {
		iv := v.Index(i)
		if iv.Kind() == reflect.Interface || iv.Kind() == reflect.Pointer {
			iv = iv.Elem()
		}
		var bsonVal any
		if iv.Kind() == reflect.Slice || iv.Kind() == reflect.Array {
			bsonVal = sliceToBsonA(iv.Interface(), structTag, omitEmpty)
		} else if iv.Kind() == reflect.Map {
			bsonVal, _ = mapToBsonD(iv.Interface(), structTag, omitEmpty)
		} else if iv.Kind() == reflect.Struct {
			bsonVal, _ = structToBsonD(iv.Interface(), structTag)
		} else {
			bsonVal = iv.Interface()
		}

		if omitEmpty && isValEmpty(bsonVal) {
			continue
		}

		res = append(res, bsonVal)
	}
	return res
}

func mapToBsonD(m any, structTag string, omitEmpty bool) (bson.D, error) {
	res := bson.D{}

	v := reflect.ValueOf(m)
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	if v.Kind() != reflect.Map {
		return nil, fmt.Errorf("Passed value of type %T. Expected a map or pointer to a map", m)
	}

	keys := v.MapKeys()
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].String() < keys[j].String()
	})

	for _, key := range keys {
		val := v.MapIndex(key)
		if val.Kind() == reflect.Interface || val.Kind() == reflect.Pointer {
			val = val.Elem()
		}

		var bsonVal any
		if val.Kind() == reflect.Slice || val.Kind() == reflect.Array {
			bsonVal = sliceToBsonA(val.Interface(), structTag, omitEmpty)
		} else if val.Kind() == reflect.Map {
			bsonVal, _ = mapToBsonD(val.Interface(), structTag, omitEmpty)
		} else if val.Kind() == reflect.Struct {
			bsonVal, _ = structToBsonD(val.Interface(), structTag)
		} else {
			bsonVal = val.Interface()
		}

		if omitEmpty && isValEmpty(bsonVal) {
			continue
		}

		res = append(res, bson.E{key.String(), bsonVal})
	}
	return res, nil
}

func structToBsonD(s any, structTag string) (bson.D, error) {
	res := bson.D{}

	v := reflect.ValueOf(s)
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Passed value of type %T. Expected a struct or pointer to a struct", s)
	}

	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}

		var columnTag *structtag.Tag
		tags, parseErr := structtag.Parse(string(f.Tag))
		if parseErr == nil {
			columnTag, _ = tags.Get(structTag)
		}

		columnName := ""
		omitEmpty := false
		if columnTag != nil {
			columnName = columnTag.Name
			if slices.Contains(columnTag.Options, "omitempty") {
				omitEmpty = true
			}
		} else {
			columnName = f.Name
		}

		fv := v.Field(i)
		if fv.Kind() == reflect.Interface || fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				continue
			}
			fv = fv.Elem()
		}

		var bsonVal any
		if fv.Kind() == reflect.Slice || fv.Kind() == reflect.Array {
			bsonVal = sliceToBsonA(fv.Interface(), structTag, omitEmpty)
		} else if fv.Kind() == reflect.Map {
			bsonVal, _ = mapToBsonD(fv.Interface(), structTag, omitEmpty)
		} else if fv.Kind() == reflect.Struct {
			bsonVal, _ = structToBsonD(fv.Interface(), structTag)
		} else {
			bsonVal = fv.Interface()
		}

		if omitEmpty && isValEmpty(bsonVal) {
			continue
		}

		res = append(res, bson.E{columnName, bsonVal})
	}
	return res, nil
}

func isValEmpty(val any) bool {
	switch val.(type) {
	case bson.A:
		bsonA := val.(bson.A)
		if len(bsonA) == 0 {
			return true
		}
	case bson.D:
		bsonD := val.(bson.D)
		if len(bsonD) == 0 {
			return true
		}
	case int, int8, int16, int32, int64, uint,
		uint8, uint16, uint32, uint64, uintptr,
		float32, float64, complex64, complex128,
		string, bool:
		return reflect.ValueOf(val).IsZero()
	}
	return false
}
