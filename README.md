# Bài thực hành nâng cao: Lập trình mạng với Apache Kafka

- **Học phần:** Lập trình mạng (Lớp 2627I_INT3304_3)
- **Giảng viên:** ThS. Kiều Hữu Thu
- **Sinh viên / Nhóm:** Nguyễn Khắc Huy - MSSV: 24022802

## Môi trường

| Thành phần | Phiên bản |
|---|---|
| Hệ điều hành | Ubuntu Server (máy ảo VMware) |
| Java | OpenJDK 17 |
| Maven | 3.x |
| Apache Kafka | 4.3.1 (KRaft, không dùng Zookeeper) |

Mô hình: 01 máy, 02 Kafka broker logic, topic `network-events` (2 partition, replication factor 2).

| Node | Broker port | Controller port | node.id |
|---|---|---|---|
| Broker 1 | 9092 | 9093 | 1 |
| Broker 2 | 9094 | 9095 | 2 |

## Cấu trúc thư mục

```
Kafka-Lab-MSSV/
├── README.md
├── report.pdf
├── screenshots/
│   ├── cluster.png
│   ├── topic.png
│   ├── producer.png
│   ├── consumer.png
│   └── ... (ảnh các thí nghiệm: tn2-*, tn3-*, tn4-*, tn5-*)
└── source/
    ├── pom.xml
    ├── lab-node1.properties
    ├── lab-node2.properties
    └── src/main/java/
        ├── EventProducer.java
        ├── EventConsumer.java
        ├── EventProducer20.java
        └── EventProducerBulk.java
```

## Cách chạy

### 1. Chuẩn bị cấu hình

Chép `lab-node1.properties` và `lab-node2.properties` vào thư mục `config` của Kafka. Sửa `log.dirs` cho đúng đường dẫn trên máy. So với đề bài, mỗi file có thêm 3 dòng (do cluster chỉ có 2 broker):

```
offsets.topic.replication.factor=2
transaction.state.log.replication.factor=2
transaction.state.log.min.isr=1
```

### 2. Khởi tạo và chạy 2 broker

```bash
cd ~/kafka
ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t $ID -c config/lab-node1.properties
bin/kafka-storage.sh format -t $ID -c config/lab-node2.properties

# Mỗi lệnh chạy ở một terminal riêng (hoặc cửa sổ tmux riêng)
bin/kafka-server-start.sh config/lab-node1.properties
bin/kafka-server-start.sh config/lab-node2.properties
```

Phải chạy đủ cả 2 broker thì cluster mới hoạt động (quorum 2 controller). Chỉ format một lần; những lần bật lại không format lại.

### 3. Tạo topic

```bash
bin/kafka-topics.sh --create --topic network-events --partitions 2 --replication-factor 2 --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic network-events --bootstrap-server localhost:9092
```

### 4. Chạy Producer / Consumer Java

```bash
cd source
mvn -q compile

# Consumer (tham số tùy chọn là group.id, mặc định network-lab-group)
mvn -q exec:java -Dexec.mainClass=EventConsumer -Dexec.args="network-lab-group"

# Producer nhập tay (gõ exit để thoát)
mvn -q exec:java -Dexec.mainClass=EventProducer

# Gửi 20 event có key (thí nghiệm 4)
mvn -q exec:java -Dexec.mainClass=EventProducer20

# Gửi 10.000 event và đo throughput
mvn -q exec:java -Dexec.mainClass=EventProducerBulk
```

## Các thí nghiệm

| Thí nghiệm | Nội dung | Ảnh |
|---|---|---|
| 1 | Producer → Kafka → Consumer | `producer.png`, `consumer.png` |
| 2 | Giao tiếp bất đồng bộ (Consumer tắt, Producer vẫn gửi) | `tn2-*` |
| 3 | Pub/Sub với 2 Consumer Group | `tn3-*` |
| 4 | Chia tải trong cùng Consumer Group | `tn4-*` |
| 5 | Gửi 10.000 event | `tn5-*` |

Chi tiết kết quả, số liệu và phần trả lời câu hỏi thảo luận nằm trong `report.pdf`.

## Lưu ý

- Mô hình 2 controller chỉ dùng cho lab, không phải kiểm thử failover hay tính sẵn sàng cao của controller quorum.
- Hai broker chạy trên cùng một máy ảo: tắt máy thì cả hai broker cùng dừng.
- Số liệu throughput chỉ mang tính gần đúng, không dùng để so sánh hiệu năng chính thức giữa các máy.
