package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// LogConfig 日志配置
type LogConfig struct {
	File    string `yaml:"file"`    // 日志文件路径
	MaxSize int    `yaml:"maxSize"` // 单个日志文件最大 MB
	MaxNum  int    `yaml:"maxNum"`  // 保留的旧日志文件数量
}

// Config 应用配置
type Config struct {
	Log LogConfig `yaml:"log"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Log: LogConfig{
			File:    "/var/log/podwatcher/podwatcher.log",
			MaxSize: 100,
			MaxNum:  3,
		},
	}
}

// Load 从配置文件加载配置，环境变量可覆盖
func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	// 从配置文件加载
	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", configPath, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	}

	// 环境变量覆盖
	if v := os.Getenv("PODWATCHER_LOG_FILE"); v != "" {
		cfg.Log.File = v
	}
	if v := os.Getenv("PODWATCHER_LOG_MAX_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Log.MaxSize = n
		}
	}
	if v := os.Getenv("PODWATCHER_LOG_MAX_NUM"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Log.MaxNum = n
		}
	}

	return cfg, nil
}
