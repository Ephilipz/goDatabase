package main

func assert(b bool) {
	if !b {
		panic("condition failed")
	}
}

func assertArrEqual[T comparable](a1, a2 []T) {
	assert(len(a1) == len(a2))
	for i:= 0; i < len(a1); i++ {
		assert(a1[i] == a2[i])	
	}
}
