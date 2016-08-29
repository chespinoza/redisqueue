package redisqueue

import "testing"

type payload struct {
	IntData    int64   `json:"intData"`
	FloatData  float64 `json:"floatData"`
	StringData string  `json:"stringData"`
}

func TestPush(t *testing.T) {
	instance := New(":6379", "")
	data := payload{
		IntData:    1,
		FloatData:  22.3,
		StringData: "test"}

	err := instance.Push("test", data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPop(t *testing.T) {
	instance := New(":6379", "")
	data := payload{}
	err := instance.Pop("test", &data)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetAll(t *testing.T) {
	instance := New(":6379", "")
	data := payload{
		IntData:    1,
		FloatData:  22.3,
		StringData: "test"}

	err := instance.Push("test", data)
	if err != nil {
		t.Fatal(err)
	}
	dat, err := instance.GetAll("test")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Result:%s\n", dat)
}
