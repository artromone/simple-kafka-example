package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type LogEvent struct {
	Timestamp int64  `json:"timestamp"`
	Level     string `json:"level"`
	Service   string `json:"service"`
	Message   string `json:"message"`
}

func main() {
	// Настраиваем consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "event-consumer-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		fmt.Printf("Ошибка при создании consumer: %s\n", err)
		os.Exit(1)
	}

	defer c.Close()

	// Подписываемся на тему
	topic := "event-logs"
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Printf("Ошибка подписки на тему: %s\n", err)
		os.Exit(1)
	}

	// Создаем файл для логов
	logFile, err := os.OpenFile("events.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Ошибка открытия файла логов: %s\n", err)
		os.Exit(1)
	}
	defer logFile.Close()

	// Канал для обработки сигналов завершения
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	run := true
	fmt.Println("Начинаем прослушивание событий. Нажмите Ctrl+C для завершения.")

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Получен сигнал %v: завершение\n", sig)
			run = false
		default:
			// Ожидаем сообщения с таймаутом
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// Получили сообщение
				var event LogEvent
				err := json.Unmarshal(e.Value, &event)
				if err != nil {
					fmt.Printf("Ошибка десериализации: %s\n", err)
					continue
				}

				// Форматируем сообщение для вывода
				timeStr := time.Unix(event.Timestamp, 0).Format("2006-01-02 15:04:05")
				logMsg := fmt.Sprintf("[%s] %s [%s] %s\n",
					timeStr, event.Level, event.Service, event.Message)

				// Выводим в консоль
				fmt.Print(logMsg)

				// Записываем в файл
				if _, err := logFile.WriteString(logMsg); err != nil {
					fmt.Printf("Ошибка записи в файл: %s\n", err)
				}

			case kafka.Error:
				// Обрабатываем ошибки Kafka
				fmt.Printf("Ошибка Kafka: %v\n", e)
			}
		}
	}
}
