package streamql

Data_Type :: enum {
	Int,
	Float,
	String,
}

Data :: union {
	i64,
	f64,
	string,
}

data_determine_type :: proc(t0, t1: Data_Type) -> Data_Type {
	if t0 == t1 {
		return t0
	}
	if t0 == .String {
		return t1
	}
	if t1 == .String {
		return t0
	}
	return .Float
}
