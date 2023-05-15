package main

func assert(b bool) {
	if !b {
		panic("condition failed")
	}
}
