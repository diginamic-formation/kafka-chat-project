#!/usr/bin/env python3

import sys
import threading
import re

from kafka import KafkaProducer, KafkaConsumer

should_quit = False
REGEX = "^[a-zA-Z0-9-]+$"
BASE_TOPIC_NAME = "chat_channel_"


def is_valid_canal_name(canal_name):
    if canal_name.startswith("#"):
        if re.match(REGEX, canal_name[1:]):
            return True
    return False


def read_messages(consumer):
    # TODO À compléter
    while not should_quit:
        # On utilise poll pour ne pas bloquer indéfiniment quand should_quit
        # devient True
        received = consumer.poll(100)

        for channel, messages in received.items():
            for msg in messages:
                print("< %s: %s" % (channel.topic, msg.value))


def cmd_msg(producer, channel, message_content, nick):
    if channel:
        topic = BASE_TOPIC_NAME + channel[1:]
        producer.send(topic,str((message_content,nick)).encode('utf-8'))
    else:
        print("Vous n'etes pas connecte")



def cmd_join(consumer, producer, line):
    canal_name = line
    if is_valid_canal_name(canal_name):
        topic = "chat_channel_" + canal_name[1:]
        print("Je m'abonne à : ", topic)
        current_subscription = consumer.subscription()
        if current_subscription:
            current_subscription.add(topic)
            consumer.subscribe(current_subscription)
        else:
            consumer.subscribe(topic)
        print(consumer.subscription())
        return True
    else:
        print("%s est un nom de canal invalide " % canal_name)
        return False

def cmd_part(consumer, producer, line):
    # TODO À compléter
    pass


def cmd_quit(producer, line):
    # TODO À compléter
    pass


def main_loop(nick, consumer, producer):
    curchan = None
    topic_list = []
    while True:
        try:
            if curchan is None:
                line = input("> ")
            else:
                line = input("[%s]> " % curchan)
        except EOFError:
            print("/quit")
            line = "/quit"

        if line.startswith("/"):
            cmd, *args = line[1:].split(" ", maxsplit=1)
            cmd = cmd.lower()
            args = None if args == [] else args[0]
        else:
            cmd = "msg"
            args = line

        if cmd == "msg":
            cmd_msg(producer, curchan, args, nick)
        elif cmd == "join":
            if cmd_join(consumer, producer, args):
                curchan = args
        elif cmd == "part":
            cmd_part(consumer, producer, args)
        elif cmd == "quit":
            cmd_quit(producer, args)
            break
        # TODO: rajouter des commandes ici



def main():
    if len(sys.argv) != 2:
        print("usage: %s nick" % sys.argv[0])
        return 1

    nick = sys.argv[1]
    consumer = KafkaConsumer()
    producer = KafkaProducer()
    th = threading.Thread(target=read_messages, args=(consumer,))
    th.start()

    try:
        main_loop(nick, consumer, producer)
    finally:
        global should_quit
        should_quit = True
        th.join()


if __name__ == "__main__":
    sys.exit(main())
