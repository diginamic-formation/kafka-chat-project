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
        producer.send(topic, str((message_content, nick)).encode('utf-8'))
    else:
        print("Vous n'etes pas connecte")


def cmd_join(consumer, producer, canal_name):
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


def cmd_part(consumer, producer, channel_name, channel_list, curchan):
    topic_name = BASE_TOPIC_NAME + channel_name[1:]
    # cas le nom existe dans la liste
    if len(channel_list) > 0 and channel_name in channel_list:
        # effacer le canal de la liste
        channel_list.remove(channel_name)
        # le nom existe et c'est le seul
        if len(channel_list) == 0:
            consumer.unsubscribe()
            curchan = None
        else:
            # on enleve le topic des abonnements
            consumer.subscription().remove(topic_name)
            consumer.subscribe(consumer.subscription())
            # si le current qui a été retiré, on le rempla par le premier de la liste
            if channel_name == curchan:
                curchan = channel_list[0]
    else:
        print("%s n'existe pas dans la liste des canaux" % channel_name)

    return curchan


def cmd_quit(producer, line):
    # TODO À compléter
    pass


def cmd_active(consumer, producer, channel_name, channel_list):
    if len(channel_list) > 0 and channel_name in channel_list:
        return True
    else:
        print("%s n'existe pas dans tes abonnements " % channel_name)
        return False


def cmd_list(channel_list):
    if len(channel_list) > 0:
        for name in channel_list:
            print(name)
    else:
        print("Votre liste d'abonnements est vide")


def main_loop(nick, consumer, producer):
    curchan = None
    channel_list = []
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
                channel_list.append(curchan)
        elif cmd == "part":
            curchan = cmd_part(consumer, producer, args, channel_list, curchan)
        elif cmd == "active":
            if cmd_active(consumer, producer, args, channel_list):
                curchan = args
        elif cmd == "list":
            cmd_list(channel_list)
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
