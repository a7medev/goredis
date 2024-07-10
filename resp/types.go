package resp

type DataType int32

const (
	SimpleStringType   DataType = 0
	SimpleErrorType    DataType = 1
	IntegerType        DataType = 2
	BulkStringType     DataType = 3
	ArrayType          DataType = 4
	NullType           DataType = 5
	BooleanType        DataType = 6
	DoubleType         DataType = 7
	BigNumberType      DataType = 8
	BulkErrorType      DataType = 9
	VerbatimStringType DataType = 10
	MapType            DataType = 11
	SetType            DataType = 12
	PushType           DataType = 13
)
