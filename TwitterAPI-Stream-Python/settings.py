TRACK_TERMS = ["#BTC", "#ETH", "#SOL", "#BNB", "#LUNA", "#NFT"]  # you can change track key words Example: Bitcoin - #BTC, or ethereum - #ETH
KAFKA_TOPIC_IN = "tweetStreaming"    # twitter API to Kafka
KAFKA_TOPIC_OUT = "visualize-tweet"    # Flink to Kafka
BOOTSTRAP_SERVERS = "<ip-kafka-host>:<port>"  # <ip kafka host>:<port> in my case, my kafka container host on Ubuntu VM so i use my IP address Ubuntu VM
try:
    from private import *
except Exception:
    pass

