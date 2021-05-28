package internal

import "testing"

func TestFillNaN(t *testing.T) {
	s := []int{8, -1, -1, -1, 20}
	s1 := FillNaN(s)

	if s1[0] != 8 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 8, s1[0])
	}
	if s1[1] != 11 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 11, s1[1])
	}
	if s1[2] != 14 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 14, s1[2])
	}
	if s1[3] != 17 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 17, s1[3])
	}
	if s1[4] != 20 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 21, s1[4])
	}

	s = []int{8, -1, -1, 20}
	s1 = FillNaN(s)

	if s1[0] != 8 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 8, s1[0])
	}
	if s1[1] != 12 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 12, s1[1])
	}
	if s1[2] != 16 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 16, s1[2])
	}
	if s1[3] != 20 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 20, s1[3])
	}

	s = []int{8, -1, -1, -1, 21}
	s1 = FillNaN(s)

	if s1[0] != 8 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 8, s1[0])
	}
	if s1[1] != 11 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 11, s1[1])
	}
	if s1[2] != 14 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 14, s1[2])
	}
	if s1[3] != 17 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 17, s1[3])
	}
	if s1[4] != 21 {
		t.Fatalf("fillNaN for s: expect %d, get %d", 21, s1[4])
	}

}
