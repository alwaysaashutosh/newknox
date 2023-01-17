package database

import (
	"fmt"
	"reflect"

	"github.com/fatih/structtag"
	"github.com/sevlyar/retag"
)

type kmuxToBsonTagger struct{}

func (tm kmuxToBsonTagger) MakeTag(st reflect.Type, idx int) reflect.StructTag {
	fieldTag := st.Field(idx).Tag

	tags, parseErr := structtag.Parse(string(fieldTag))
	if parseErr != nil {
		return fieldTag
	}

	kmuxTag, _ := tags.Get(KmuxColumnTag)
	if kmuxTag != nil {
		bsonTag := structtag.Tag{
			Key:  "bson",
			Name: kmuxTag.Name,
		}
		if kmuxTag.HasOption("omitempty") {
			bsonTag.Options = append(bsonTag.Options, "omitempty")
		}
		bsonTagStr := bsonTag.String()
		return reflect.StructTag(bsonTagStr)
	}

	return fieldTag
}

func getRetaggedStruct(val any, tagger retag.TagMaker) any {
	return retag.Convert(val, tagger)
}

func makeRetaggedSlice(val any, tagger retag.TagMaker) (any, error) {
	v := reflect.ValueOf(val)
	if v.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("Passed value of type %T. Expected pointer to []struct", val)
	}

	v = v.Elem()
	if v.Kind() != reflect.Slice {
		return nil, fmt.Errorf("Passed value of type %T. Expected pointer to []struct", val)
	}

	elemType := v.Type().Elem()
	if elemType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Passed value of type %T. Expected pointer to []struct", val)
	}

	srcStruct := reflect.New(elemType).Interface()

	retagStruct := retag.Convert(srcStruct, tagger)

	retagSliceType := reflect.SliceOf(reflect.TypeOf(retagStruct).Elem())

	return reflect.New(retagSliceType).Interface(), nil
}

func copySliceElements(dst, src any) error {
	dstPtr := reflect.ValueOf(dst)
	srcPtr := reflect.ValueOf(src)
	if dstPtr.Kind() != reflect.Pointer {
		return fmt.Errorf("dst is a value of type %T. Expected pointer to a slice", dst)
	}
	if srcPtr.Kind() != reflect.Pointer {
		return fmt.Errorf("src is a value of type %T. Expected pointer to a slice", src)
	}

	dstSlice := dstPtr.Elem()
	srcSlice := srcPtr.Elem()
	if dstSlice.Kind() != reflect.Slice {
		return fmt.Errorf("dst is a value of type %T. Expected pointer to a slice", dst)
	}
	if srcSlice.Kind() != reflect.Slice {
		return fmt.Errorf("src is a value of type %T. Expected pointer to a slice", src)
	}

	newDstSlice := dstSlice.Slice(0, 0)
	dstElemType := dstSlice.Type().Elem()

	for i := 0; i < srcSlice.Len(); i++ {
		srcElem := srcSlice.Index(i)
		dstElem := reflect.NewAt(dstElemType, srcElem.Addr().UnsafePointer()).Elem()
		newDstSlice = reflect.Append(newDstSlice, dstElem)
	}

	dstSlice.Set(newDstSlice)
	return nil
}
