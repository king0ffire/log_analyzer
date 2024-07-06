from pyshark.capture.capture import TSharkCrashException
import pyshark
                      
def pcapInfoToListBy2Filters(filename, filter1, filter2):
    csvlist=[]
    display_filter=filter1+"||"+filter2
    field1=filter1.split('.')[-1].lower()
    field2=filter2.split('.')[-1].lower()
    try:
        with pyshark.FileCapture(filename,only_summaries=True,display_filter=display_filter) as capsummary:
            try:
                with pyshark.FileCapture(filename,only_summaries=False,display_filter=display_filter) as capdetail: 
                    for packetsummary, packetdetail in zip(capsummary, capdetail): 
                            csvlist.append([packetsummary.no,packetdetail.sniff_time,
                                            packetsummary.source,packetsummary.destination,
                                            packetsummary.protocol,packetsummary.info,
                                            packetdetail.s1ap.get_field_value(field1),
                                            packetdetail.s1ap.get_field_value(field2)])
            except TSharkCrashException:
                print("detail cap catched")
    except TSharkCrashException:
            print("summary cap catched")
    print("print \"%s\" successful"%filename)
    return csvlist



def process_one_file_by2filters(csvwriter,filename,filter1,filter2):
    csvwriter.writerows(pcapInfoToListBy2Filters(filename,filter1,filter2))