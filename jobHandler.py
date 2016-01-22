import argparse, os, sys, subprocess, re, time
from Queue import Queue
from threading import Thread

parser = argparse.ArgumentParser(description='Manage a number of tasks requiring transfer of files and submission to genesis, movement of important files back to filer, and garbage removal.')

parser.add_argument('commands', help='A list of commands to run. Any files required by each command should NOT be placed within the command itself, but should instead use a placeholder defined by the "--placeholder" argument. Each command should begin with the ">" character, and any files required by the command should be placed in order of appearance within the command below the line containing said command. Also make sure that any executables used in any of the commands are present in the $PATH of the cluster, or are sourced in the command itself.')
parser.add_argument('outdir', help='Directory to output to on the cluster')
parser.add_argument('-p', '--placeholder', default='?', help='The character used to indicate a file within a command. Default is "?"')
parser.add_argument('-qs', '--queue_size', type=int, default=20, help='The number of commands to be running concurrently. Default is 20.')
#parser.add_argument()
#parser.add_argument()
#parser.add_argument()

args = parser.parse_args()

def parseCommands(commands):
    result = []
    with open(commands, 'r') as f:
        header = []
        for line in f:
            line = line.strip()
            if line[0] == '>':
                if not header:
                    header.append(line[1:].split())
                    continue
                else:
                    result.append(header)
                    header = [line[1:].split()]
                    continue
            header.append(line)
        result.append(header)
    return result

def submitJob(command):
    c = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o,e = c.communicate()
    jid = o.split()[2]
    return jid

def delJob(jid, cluster='genesis'):
    command = 'ssh {} qdel {}'.format(cluster, jid)
    c = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o,e = c.communicate()
    return o

def checkJob(jid, cluster='genesis'):
    command = 'ssh {} qstat | grep {}'.format(cluster, jid)
    c = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o,e = c.communicate()
    res = o.split()
    return res

def waitJob(jid):
    r = checkJob(jid)
    while r:
        time.sleep(5)
        r = checkJob(jid)
    return True

def transfer(_file, dest):
    command = 'ssh apollo qsub /home/dmacmillan/scripts/bash/transfer_file.sh {} {}'.format(_file, dest)
    c = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o,e = c.communicate()
    jid = o.split()[2]
    return command, jid

parsed = parseCommands(args.commands)
q = Queue()

#c, jid = transfer('/projects/dmacmillanprj2/polya/ccle/STAR/config', '/genesis/home/dmacmillan')
