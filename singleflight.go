package main

// Кеш истёк для популярного ключа "chat:1"

// Одновременно 1000 запросов:
//   Поток 1: кеш пуст → запрос в БД
//   Поток 2: кеш пуст → запрос в БД
//   Поток 3: кеш пуст → запрос в БД
//   ...
//   Поток 1000: кеш пуст → запрос в БД

// БД получает 1000 одинаковых запросов → перегрузка!

// С singleflight:

// 1000 запросов → 1 запрос в БД ✅

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type Chat struct {
}

type Cache struct {
	redis *redis.Client
	ttl   time.Duration
	group singleflight.Group // ← Ключевой компонент
}

func NewCache(rdb *redis.Client, ttl time.Duration) *Cache {
	return &Cache{
		redis: rdb,
		ttl:   ttl,
	}
}

func (c *Cache) GetChat(ctx context.Context, chatID int64, dbFetcher func(int64) (*Chat, error)) (*Chat, error) {
	key := fmt.Sprintf("chat:%d", chatID)

	// 1. Проверяем кеш
	data, err := c.redis.Get(ctx, key).Bytes()
	if err == nil {
		var chat Chat
		json.Unmarshal(data, &chat)
		return &chat, nil
	}

	// 2. singleflight — только ОДИН запрос в БД
	result, err, _ := c.group.Do(key, func() (interface{}, error) {
		// Этот код выполнится ОДИН раз
		// Остальные 999 потоков ждут результат

		// Проверяем кеш ещё раз (мог заполниться пока ждали)
		data, err := c.redis.Get(ctx, key).Bytes()
		if err == nil {
			var chat Chat
			json.Unmarshal(data, &chat)
			return &chat, nil
		}

		// Идём в БД
		chat, err := dbFetcher(chatID)
		if err != nil {
			return nil, err
		}

		// Кешируем
		data, _ = json.Marshal(chat)
		c.redis.Set(ctx, key, data, c.ttl)

		return chat, nil
	})

	if err != nil {
		return nil, err
	}

	return result.(*Chat), nil
}
