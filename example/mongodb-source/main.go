// Package main
package main

import (
	"fmt"
	"os"

	"github.com/accuknox/kmux"
	"github.com/accuknox/kmux/config"
	"github.com/accuknox/kmux/database"
)

type l1 struct {
	Value string `kmux:"value"`
	L2    l2     `kmux:"l2"`
}

type l2 struct {
	Value string `kmux:"value"`
	L3    l3     `kmux:"l3"`
}

type l3 struct {
	Value int `kmux:"value"`
}

func main() {
	// Init
	err := kmux.Init(&config.Options{
		LocalConfigFile: "kmux-config.yaml",
	})
	exitOnError(err)

	// Create new database source
	ds, err := kmux.NewDatabaseSource()
	exitOnError(err)

	// Connect with the source
	err = ds.Connect()
	exitOnError(err)

	// Source.Get() API examples
	getExample1(ds)
	getExample2(ds)
	getExample3(ds)
	countExample(ds)

	// Disconnect from the source
	ds.Disconnect()
}

func getExample1(ds database.Source) {
	res := []l1{} // (OR) res := []map[string]any{}

	// 1. The first arg in Get() points to the mongoDB collection name
	// 2. `Rows` should be a reference to slice of struct which has `kmux`
	//    tags (OR) it can be a reference to slice of `map[string]any`.
	// 3. `Where` and `Args` are optional fields which can used to filter
	//     the records added to `Rows`.
	err := ds.Get("test", database.Query{
		Rows:  &res,
		Where: "l2.l3.value = ?",
		Args:  []any{204},
	})

	exitOnError(err)

	fmt.Printf("\nTotal Records --> %d\n", len(res))

	for i, rec := range res {
		fmt.Printf("%d --> %#v\n", i+1, rec)
	}
}

func getExample2(ds database.Source) {
	res := []l1{}

	// `Limit` and `Offset` are optional field which can used to
	// limit and skip records added to `Rows`.
	err := ds.Get("test", database.Query{
		Rows:   &res,
		Limit:  10,
		Offset: 5,
	})

	exitOnError(err)

	fmt.Printf("\nTotal Records --> %d\n", len(res))

	for i, rec := range res {
		fmt.Printf("%d --> %#v\n", i+1, rec)
	}
}

func getExample3(ds database.Source) {
	res := []map[string]any{}

	// 1. `GroupBy` is an option field which can be used identify distinct
	//    value of a column or distinct combinations of two or more columns.
	// 2. `OrderBy` is an optional field which can used to sort the records
	//    based on one or more columns.
	err := ds.Get("test", database.Query{
		Rows:    &res,
		GroupBy: []string{"l2.l3.value"},
		OrderBy: []database.OrderByColumn{{Name: "l2.l3.value", Desc: false}},
	})

	exitOnError(err)

	fmt.Printf("\nTotal Records --> %d\n", len(res))

	for i, rec := range res {
		fmt.Printf("%d --> %v\n", i+1, rec)
	}
}

func countExample(ds database.Source) {
	// Count() returns total no. of records that matches a particular query.
	count, err := ds.Count("test", database.Query{
		Where: "l2.l3.value = ?",
		Args:  []any{204},
	})

	exitOnError(err)

	fmt.Printf("\nTotal count of records --> %d\n", count)
}

func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
