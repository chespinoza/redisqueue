package redisqueue

import (
	"encoding/json"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	redisPool *redis.Pool
)

func newRedisPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

type RedisQueue struct {
	redisPool *redis.Pool
}

//New Create a new Redis Queue Instance
func New(server, pass string) *RedisQueue {
	redisQueue := RedisQueue{}
	redisQueue.redisPool = newRedisPool(server, pass)
	return &redisQueue
}

//Push serialize an item object and push it into the queue
func (r RedisQueue) Push(queueName string, item interface{}) error {
	pool := r.redisPool.Get()
	defer pool.Close()
	d, err := json.Marshal(item)
	if err != nil {
		return err
	}
	if _, err = pool.Do("RPUSH", queueName, d); err != nil {
		return err
	}
	return nil
}

//Pop get one item from queue, and deserialize it to item object
func (r RedisQueue) Pop(queueName string, item interface{}) (err error) {
	var response string
	pool := r.redisPool.Get()
	defer pool.Close()
	if response, err = redis.String(pool.Do("LPOP", queueName)); err != nil {
		return err
	}
	err = json.Unmarshal([]byte(response), &item)
	if err != nil {
		return err
	}
	return nil
}

//GetAll_ ...
func (r RedisQueue) GetAll_(queueName string, items []interface{}) (err error) {
	var strs []string
	var values []interface{}

	pool := r.redisPool.Get()
	defer pool.Close()
	if values, err = redis.Values(pool.Do("LRANGE", queueName, 0, -1)); err != nil {
		return err
	}
	if err := redis.ScanSlice(values, &strs); err != nil {
		return err
	}
	items = make([]interface{}, len(strs))
	for i, v := range strs {
		err = json.Unmarshal([]byte(v), &items[i])
		if err != nil {
			return err
		}

	}

	return nil
}

//GetAll return all fields but in string format
func (r RedisQueue) GetAll(queueName string) (items []string, err error) {
	pool := r.redisPool.Get()
	defer pool.Close()
	if items, err = redis.Strings(pool.Do("LRANGE", queueName, 0, -1)); err != nil {
		return items, err
	}
	return items, nil
}

//Clean will empty the queue
func (r RedisQueue) Clean(queueName string) error {
	pool := r.redisPool.Get()
	defer pool.Close()
	if _, err := pool.Do("DEL", queueName); err != nil {
		return err
	}
	return nil
}
