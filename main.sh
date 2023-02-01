#/bin/bash/

path="/media/james/Database/pair_raw/"
# step1 download file
python3 ${path}download_pair_file.py
return_code1=$?
if [ ${return_code1} -ne 0 ]
then
    python3 ${path}send_msg.py "[Pair Data] WARNING: Step1 Fail"
else
    # remove zip file
    rm ${path}data/*zip
    # step2 upsert file
    #echo Step2 skip
    python3 ${path}pair_upsert_mongodb.py
    return_code2=$?
    if [ ${return_code2} -ne 0 ]
    then
        python3 ${path}send_msg.py "[Pair Data] WARNING: Step2 Fail"
    else
        # step3 error fix
        python3 ${path}pair_error_fixed.py
        return_code3=$?
        rm ${path}log/missing.txt
        if [ ${return_code3} -ne 0 ]
        then
            python3 ${path}send_msg.py "[Pair Data] WARNING: Step3 Fail"
        else
            python3 ${path}send_msg.py "[Pair Data] INFO: Pair data upsert success"
            rm ${path}data/*json
        fi
    fi
fi