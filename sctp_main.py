import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
import logging.handlers
import queue
import os
import csv
import glob
import gzip
import sys
import logging

def mapget(map, key):
    if key in map:
        return map[key]
    else:
        return 0

def sctpanalysis(csvfile_id,csvwriter_id, sctp_file_list,cache_path,filter1,filter2, mode=0):
    from ids_pyshark import pcapInfoToListBy2Filters, process_one_file_by2filters
    logger=logging.getLogger(__name__)
    logger.info("sctp started")
    csvwriter_id.writerow(
        [
            "Filename",
            "Pkt Num",
            "Time",
            "Source IP",
            "Destination IP",
            "Protocol",
            "Summary Info",
            "MME-ID",
            "ENB-ID",
        ]
    ) 
    for i, filename in enumerate(sctp_file_list):
        if filename[-3:] == ".gz":
            with gzip.open(filename) as f:
                with open(
                    os.path.join(
                        cache_path, os.path.splitext(os.path.basename(filename))[0]
                    ),
                    "wb",
                ) as f2:
                    f2.write(f.read())
                    sctp_file_list[i] = f2.name
    if mode == 0:
        for filename in sctp_file_list:
            # csvwriter_id.writerow([os.path.basename(filename),'','','','','','',''])
            process_one_file_by2filters(csvwriter_id, filename, filter1, filter2)
            csvfile_id.flush()
            print("sctp_finished_one")
            sys.stdout.flush()
    elif mode == 1:
        with ThreadPoolExecutor() as executor:
            fs = [
                executor.submit(
                    pcapInfoToListBy2Filters,
                    filename,
                    filter1,
                    filter2,
                    asyncio.new_event_loop(),
                )
                for filename in sctp_file_list
            ]
            for future in as_completed(fs):
                csvwriter_id.writerows(future.result())
                csvfile_id.flush()
                print("sctp_finished_one")
                sys.stdout.flush()
        print("multithread success")
    elif mode == 2:
        with ProcessPoolExecutor() as executor:
            fs = [
                executor.submit(
                    pcapInfoToListBy2Filters,
                    filename,
                    filter1,
                    filter2,
                )
                for filename in sctp_file_list
            ]
            for future in as_completed(fs):
                csvwriter_id.writerows(future.result())
                csvfile_id.flush()
                print("sctp_finished_one")
                sys.stdout.flush()
        print("multithread success")
    logger.info("sctp finished")


# mode 0 is single thread， mode 1 is multithread
def run(filelocation, mode=0):
    filter1 = "s1ap.MME_UE_S1AP_ID"
    filter2 = "s1ap.ENB_UE_S1AP_ID"

    extracteddir = os.path.splitext(os.path.splitext(filelocation)[0])[0]
    fileuid=os.path.basename(extracteddir)
    if not os.path.exists(extracteddir):
        os.makedirs(extracteddir)

    queue_listener = configure_logger(extracteddir)
    queue_listener.start()
    
    logger = logging.getLogger(__name__)
    logger.info("start logger")
    logger.info("Current Working Directory: %s", os.getcwd())
    logger.info("Current File Dirctory: %s", os.path.abspath("."))

    tracelocation = os.path.join(extracteddir, "logs", "trace.tgz")
    traceextraceteddir = os.path.splitext(tracelocation)[0]
    if not os.path.exists(traceextraceteddir):
        os.makedirs(traceextraceteddir)
        

    csvfile_id = open(os.path.join(extracteddir, "ids.csv",), "w", newline="",encoding="utf-8")
    csvwriter_id = csv.writer(csvfile_id)
    sctp_file_list = glob.glob(os.path.dirname(tracelocation) + "/sctp*")

    cache_path = os.path.join(extracteddir, "cache")
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
    
    print(len(sctp_file_list))
    sctpanalysis( csvfile_id, csvwriter_id, sctp_file_list, cache_path, filter1,filter2,mode)
    
    queue_listener.stop()


def configure_logger(location):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    que = queue.Queue(-1)
    queue_handler = logging.handlers.QueueHandler(que)
    file_handler = logging.FileHandler(os.path.join(location, "python_sctp.log"), mode="w")
    queue_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(threadName)s %(message)s")
    )

    queuelistener = logging.handlers.QueueListener(que, file_handler)
    logger.addHandler(queue_handler)
    return queuelistener


if __name__ == "__main__":
    run(sys.argv[1], mode=int(sys.argv[2]))

