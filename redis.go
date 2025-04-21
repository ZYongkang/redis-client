package redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

// RedisConfig 用于存储 Redis 配置
type RedisConfig struct {
	IsCluster bool     `mapstructure:"is_cluster"`
	Nodes     []string `mapstructure:"nodes"` // 用于 Cluster 模式
	Addr      string   `mapstructure:"addr"`
	Password  string   `mapstructure:"password"`
	DB        int      `mapstructure:"db"`
}

// Client 是全局的 Redis 客户端
var (
	Client        redis.UniversalClient
	ClusterClient *redis.ClusterClient
	config        RedisConfig
)

// NewRedisConfig 从配置文件读取 Redis 配置
func NewRedisConfig(filePath string, fileName string, format string) (*RedisConfig, error) {
	viper.SetConfigName(fileName) // 配置文件名 (不带扩展名)
	viper.SetConfigType(format)   // 配置文件类型
	viper.AddConfigPath(filePath) // 配置文件路径

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config RedisConfig
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %v", err)
	}

	return &config, nil
}

// NewRedisClient 初始化 Redis 客户端
func NewRedisClient(ctx context.Context, config *RedisConfig) error {
	if config.IsCluster {
		return initClusterClient(ctx, config)
	}
	return initSingleClient(ctx, config)
}

// initSingleClient 初始化单机模式 Redis 客户端
func initSingleClient(ctx context.Context, config *RedisConfig) error {
	Client = redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Password,
		DB:       config.DB,
	})

	if err := Client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %v", err)
	}

	fmt.Println("Connected to Redis in single node mode")
	return nil
}

// initClusterClient 初始化 Cluster 模式 Redis 客户端
func initClusterClient(ctx context.Context, config *RedisConfig) error {
	Client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.Nodes,
		Password: config.Password,
	})
	ClusterClient = Client.(*redis.ClusterClient)

	if err := Client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis Cluster: %v", err)
	}

	fmt.Println("Connected to Redis in cluster mode")
	return nil
}

// GetClient 返回 Redis 客户端
func GetClient() redis.UniversalClient {
	return Client
}

func Scan(ctx context.Context, pattern string, count int64, fn func(keys []string) error) error {
	if config.IsCluster {
		err := ClusterClient.ForEachMaster(ctx, func(context context.Context, master *redis.Client) error {
			var cursor uint64 = 0
			for {
				k, c, err := master.Scan(context, cursor, pattern, count).Result()
				if err != nil {
					fmt.Println("Error scanning keys: ", err)
				}

				// 将符合条件的键放入 channel 中
				err = fn(k)
				if err != nil {
					return err
				}
				// 如果 cursor 为 0，表示扫描完成
				if c == 0 {
					fmt.Println("Scan completed")
					break
				}
				cursor = c
			}
			return nil
		})
		return err
	} else {
		var cursor uint64 = 0
		for {
			keys, c, err := Client.Scan(ctx, cursor, pattern, count).Result()
			if err != nil {
				fmt.Println("Error scanning keys: ", err)
				return err
			}
			err = fn(keys)
			if err != nil {
				return err
			}
			if c == 0 {
				fmt.Println("Scan completed")
				break
			}
			cursor = c
		}
		return nil
	}
}

func Type(ctx context.Context, key string) (string, error) {
	if config.IsCluster {
		result, err := ClusterClient.Type(ctx, key).Result()
		if err != nil {
			return "", fmt.Errorf("failed to get type of key %s: %v", key, err)
		}
		if result == "none" {
			return "", fmt.Errorf("key %s does not exist", key)
		}
		return result, nil

	} else {
		typ, err := Client.Type(ctx, key).Result()
		if err != nil {
			return "", fmt.Errorf("failed to get type of key %s: %v", key, err)
		}
		if typ == "none" {
			return "", fmt.Errorf("key %s does not exist", key)
		}
		return typ, nil
	}
}

func Get(ctx context.Context, key string) (string, error) {
	if config.IsCluster {
		result, err := ClusterClient.Get(ctx, key).Result()
		if err != nil {
			return "", fmt.Errorf("failed to get value of key %s: %v", key, err)
		}
		return result, nil
	} else {
		result, err := Client.Get(ctx, key).Result()
		if err != nil {
			return "", fmt.Errorf("failed to get value of key %s: %v", key, err)
		}
		return result, nil
	}
}
