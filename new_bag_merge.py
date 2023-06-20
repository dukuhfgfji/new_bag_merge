from sqlite3 import Time
import sys
import rosbag
import time
import os
import argparse
import numpy as np
import rospy
from std_msgs.msg import Float64
from colorama import Fore, Back, Style

MODE = ""

SYNC_ACCYRACY = 0.0
SYNC_RANGE = 0.0
EXCLUDE_TOPIC = ""

DELTA_TIME_TOPIC = ""
DELTA_TIME_DICT = {}


def INFO():
    return Fore.GREEN + "[INFO]   " + Style.RESET_ALL + " "


def WARNING():
    return Fore.YELLOW + "[WARNING]" + Style.RESET_ALL + " "


def ERROR():
    return Fore.RED + "[ERROR]  " + Style.RESET_ALL + " "


def status(length, percent):
    sys.stdout.write("\x1B[2K")  # Erase entire current line
    sys.stdout.write("\x1B[0E")  # Move to the beginning of the current line
    progress = Fore.GREEN + "[PROGRESS]" + Style.RESET_ALL + "["
    for i in range(0, length):
        if i < length * percent:
            progress += "="
        else:
            progress += " "
    progress += "] " + str(round(percent * 100.0, 2)) + "%"
    sys.stdout.write(progress)
    sys.stdout.flush()


def checkTopic(string):
    if EXCLUDE_TOPIC == "":
        return True
    if not EXCLUDE_TOPIC in string:
        return True
    else:
        return False


def bagSync(inbag, outbag, namespace):
    d_t = DELTA_TIME_DICT[inbag]

    ref_t = 0

    prev_msg = {}
    prev_t = {}

    for topic in inbag.get_type_and_topic_info()[1].keys():
        prev_msg[topic] = None
        prev_t[topic] = 0

    for topic, msg, t in inbag.read_messages():
        sync_t = t.to_sec() + d_t
        if checkTopic(topic):
            if MODE == "async":
                yield sync_t
                outbag.write(
                    "/" + namespace + topic,
                    msg,
                    rospy.Time().from_sec(sync_t),
                )

            elif MODE == "sync":
                while sync_t > ref_t:
                    ref_t = yield
                outbag.write(
                    "/" + namespace + topic,
                    msg,
                    rospy.Time().from_sec(sync_t),
                )

            elif MODE == "ad sync":
                while sync_t > ref_t + SYNC_ACCYRACY:
                    ref_t = yield

                if sync_t > ref_t and prev_t[topic] < ref_t:
                    if abs(sync_t - ref_t) < abs(prev_t[topic] - ref_t):
                        if abs(sync_t - ref_t) < SYNC_RANGE:
                            outbag.write(
                                "/" + namespace + topic,
                                msg,
                                rospy.Time().from_sec(ref_t),
                            )
                    else:
                        if abs(prev_t[topic] - ref_t) < SYNC_RANGE:
                            outbag.write(
                                "/" + namespace + topic,
                                prev_msg[topic],
                                rospy.Time().from_sec(ref_t),
                            )

                prev_msg[topic] = msg
                prev_t[topic] = sync_t

    while True:
        yield 9999999999


def main(args):
    parser = argparse.ArgumentParser(
        description="Bags sync/async merge script based on timestamps of bags.\
          Notice that bag timestamp is different from message timestamp.\
          The bag timestamp is the time when the message is written into the bag, while the message timestamp is in the message header.\
          Bag name will be used as namespace for topics in the bag."
    )
    parser.add_argument(
        "bagfile", nargs="+", help="input bag file, bag name will be namespace"
    )
    parser.add_argument(
        "-a",
        "--sync_accuracy",
        nargs=1,
        type=float,
        default=0.0,
        help="time sync step accuracy in second, set to 0 to use async mode, or set to a value to use sync mode",
    )
    parser.add_argument(
        "-r",
        "--sync_range",
        nargs=1,
        type=float,
        default=0.0,
        help="Used in sync mode in second, set to 0 to disable, messages in sync range will be forced to be the same timestamp.",
    )
    parser.add_argument(
        "-d",
        "--delta_time_topic",
        nargs=1,
        type=str,
        default="",
        help="Delta time topic name, std_msgs/Float64, delta_time = device_time - system_time",
    )
    parser.add_argument(
        "-x",
        "--exclude_topics",
        nargs=1,
        type=str,
        default="",
        help="Exclude topics having the follow string.",
    )

    args = parser.parse_args()

    bagFileList = args.bagfile
    bagList = []
    syncList = []

    start_time = 9999999999
    end_time = 0

    global SYNC_ACCYRACY
    global SYNC_RANGE
    global EXCLUDE_TOPIC
    global MODE

    if isinstance(args.sync_accuracy, list):
        SYNC_ACCYRACY = float(args.sync_accuracy[0])
    else:
        SYNC_ACCYRACY = float(args.sync_accuracy)

    if isinstance(args.sync_range, list):
        SYNC_RANGE = float(args.sync_range[0])
    else:
        SYNC_RANGE = float(args.sync_range)

    if isinstance(args.delta_time_topic, list):
        DELTA_TIME_TOPIC = str(args.delta_time_topic[0])
    else:
        DELTA_TIME_TOPIC = str(args.delta_time_topic)

    if isinstance(args.exclude_topics, list):
        EXCLUDE_TOPIC = str(args.exclude_topics[0])
    else:
        EXCLUDE_TOPIC = str(args.exclude_topics)

    if not SYNC_ACCYRACY > 0:
        MODE = "async"
        print(INFO() + "run in ASYNC mode")
    else:
        MODE = "sync"
        print(INFO() + "Run in SYNC mode", end=", ")
        print("sync accuracy = " + str(SYNC_ACCYRACY) + "s")
        if SYNC_RANGE > 0:
            MODE = "ad sync"
            print(INFO() + "ADVANCED SYNC enabled", end=", ")
            print("sync range = " + str(SYNC_RANGE) + "s")

    with rosbag.Bag("output.bag", "w") as outbag:
        for bagFile in bagFileList:
            bag = rosbag.Bag(bagFile, "r")

            if not DELTA_TIME_TOPIC == "":
                dtTopic = bag.read_messages(DELTA_TIME_TOPIC)
                try:
                    tempTopic, tempMsg, tempT = dtTopic.send(None)
                    DELTA_TIME_DICT[bag] = tempMsg.data
                    print(
                        INFO()
                        + "In "
                        + bagFile
                        + ", "
                        + DELTA_TIME_TOPIC
                        + " found, delta time = "
                        + str(tempMsg.data)
                        + "s"
                    )
                except:
                    DELTA_TIME_DICT[bag] = 0
                    print(
                        INFO()
                        + "In "
                        + bagFile
                        + ", "
                        + DELTA_TIME_TOPIC
                        + " not found or error"
                    )

            bagList.append(bag)
            syncList.append(bagSync(bag, outbag, os.path.basename(bagFile)[:-4]))

        if DELTA_TIME_TOPIC == "":
            for bag in bagList:
                DELTA_TIME_DICT[bag] = 0
        elif 0 in DELTA_TIME_DICT.values():
            print(
                WARNING()
                + DELTA_TIME_TOPIC
                + " not found or error in some bags, use 0s instead"
            )
            for key in DELTA_TIME_DICT:
                DELTA_TIME_DICT[key] = 0

        try:
            d_t_ref = DELTA_TIME_DICT[bagList[0]]
            for bag in bagList:
                d_t = d_t_ref - DELTA_TIME_DICT[bag]
                DELTA_TIME_DICT[bag] = d_t
                if start_time > bag.get_start_time() + d_t:
                    start_time = bag.get_start_time() + d_t
                if end_time < bag.get_end_time() + d_t:
                    end_time = bag.get_end_time() + d_t
        except:
            print(ERROR() + "Empty bag detected: " + str(bag.filename))
            return

        for sync in syncList:
            sync.send(None)

        duration = end_time - start_time

        last_time = time.perf_counter()
        if MODE == "async":
            timestampDict = {}
            for sync in syncList:
                timestampDict[sync] = sync.send(None)
            while True:
                index = min(timestampDict, key=timestampDict.get)
                if timestampDict[index] > end_time:
                    break
                timestampDict[index] = index.send(None)

                if time.perf_counter() - last_time > 0.1:
                    percent = (timestampDict[index] - start_time) / duration
                    status(40, percent)
                    last_time = time.perf_counter()

        elif MODE == "sync" or MODE == "ad sync":
            startNum = int(start_time // SYNC_ACCYRACY)
            endNum = int(end_time // SYNC_ACCYRACY + 2)
            for num in range(startNum, endNum):
                for sync in syncList:
                    sync.send(num * SYNC_ACCYRACY)

                if time.perf_counter() - last_time > 0.1:
                    percent = (num - startNum) / (endNum - startNum)
                    status(40, percent)
                    last_time = time.perf_counter()

    status(40, 1.0)
    print("")

    for bag in bagList:
        bag.close()


if __name__ == "__main__":
    main(sys.argv[1:])
