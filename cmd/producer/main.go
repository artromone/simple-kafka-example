package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
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
	// Настраиваем producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "event-producer",
		"acks":              "all",
	})
	if err != nil {
		fmt.Printf("Ошибка при создании producer: %s\n", err)
		os.Exit(1)
	}

	defer p.Close()

	// Канал для обработки событий доставки сообщений
	deliveryChan := make(chan kafka.Event)

	// Обработка сигналов для корректного завершения
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt)

	// Уровни логирования
	levels := []string{"INFO", "WARNING", "ERROR"}

	// Сервисы
	services := []string{"auth-service", "payment-service", "user-service"}

	// Сообщения
	messages := map[string][]string{
		"INFO":    {"Пользователь вошел в систему", "Операция выполнена успешно", "Запрос обработан"},
		"WARNING": {"Повторная попытка операции", "Медленный ответ от БД", "Ресурс близок к исчерпанию"},
		"ERROR":   {"Сбой аутентификации", "Ошибка соединения с БД", "Недостаточно прав доступа"},
	}

	topic := "event-logs"
	counter := 0

	fmt.Println("Начинаем отправку событий. Нажмите Ctrl+C для завершения.")

	ticker := time.NewTicker(2 * time.Second)
	run := true

	for run {
		select {
		case <-ticker.C:
			// Генерируем случайное событие
			level := levels[rand.Intn(len(levels))]
			service := services[rand.Intn(len(services))]
			message := messages[level][rand.Intn(len(messages[level]))]

			event := LogEvent{
				Timestamp: time.Now().Unix(),
				Level:     level,
				Service:   service,
				Message:   message,
			}

			// Преобразуем в JSON
			eventJSON, err := json.Marshal(event)
			if err != nil {
				fmt.Printf("Ошибка сериализации: %s\n", err)
				continue
			}

			// Отправляем сообщение в Kafka
			err = p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          eventJSON,
				Key:            []byte(fmt.Sprintf("event-%d", counter)),
			}, deliveryChan)
			if err != nil {
				fmt.Printf("Ошибка отправки: %s\n", err)
				continue
			}

			counter++
			fmt.Printf("Отправлено событие [%s]: %s - %s\n", level, service, message)

		case e := <-deliveryChan:
			// Проверяем результат доставки
			m := e.(*kafka.Message)
			if m.TopicPartition.Error != nil {
				fmt.Printf("Ошибка доставки: %v\n", m.TopicPartition.Error)
			}

		case <-sigchan:
			fmt.Println("Завершение работы...")
			run = false
		}
	}

	// Ждем, пока все сообщения будут отправлены
	p.Flush(15 * 1000)
}
