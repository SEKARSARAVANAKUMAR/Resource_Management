if ! ps ax | grep -q "[W]ebApplication.py"; then
    echo "Restarting KBS WebApplication :  $(date)" >> /home/centos/AITest/Logo_Detection/WebAppStart.log
    cd /home/centos/AITest/Logo_Detection
    nohup /usr/bin/python /home/centos/AITest/Logo_Detection/WebApplication.py &
fi
