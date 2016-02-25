import traceback
import argparse, os, sys, subprocess, re, time, random, string, shutil
from Queue import Queue
from threading import Thread

parser = argparse.ArgumentParser(description='Manage a number of tasks requiring transfer of files and submission to genesis, movement of important files back to filer, and garbage removal.')

parser.add_argument('config', help='A configuration file with the following format. Each line contains 3 columns separated by tabs, column 1 is the name of the job (must be unique!). Column 2 is the command itself, but with local files using by the command replaced by the "--placeholder" character. If there is a flag to specify an output directory, use "<output>" instead of the directory. Column 3 contains a comma-separated list of the files required in order of appearance in the command.')
parser.add_argument('outdir', help='Directory to output to on the cluster')
parser.add_argument('indir', help='The local directory to store completed jobs in')
parser.add_argument('-p', '--placeholder', default='?', help='The character used to indicate a file within a command. Default is "?"')
parser.add_argument('-qs', '--queue_size', type=int, default=20, help='The number of commands to be running concurrently. Default is 20.')
parser.add_argument('-ncpus', type=int, default=12, help='The number of CPUs to use per job (max 12). Default = 1.')
parser.add_argument('-mem', default='3.83G', help='The memory to use per job. Default is "3.83G".')
parser.add_argument('-fn', '--filename', action='store_true', help='If your command requires a filename rather than an output directory, enable this flag.')
#parser.add_argument()
#parser.add_argument()
#parser.add_argument()

args = parser.parse_args()

class Command:
    def __init__(self, name, command, files):
        self.name = name
        self.cmd = command
        self.files = files

    def __str__(self):
        return '{}\t{}\t{}'.format(self.name,self.cmd,self.files)
    def __repr__(self):
        return '{}\t{}\t{}'.format(self.name,self.cmd,self.files)

def sprint(s):
    sys.stdout.write(s)
    sys.stdout.flush()

def parseConfig(config):
    commands = []
    with open(config, 'r') as f:
        for line in f:
            name, command, files = line.strip().split('\t')
            command = command.split()
            files = files.split(',')
            c = Command(name, command, files)
            commands.append(c)
    return commands

# Create the actual string for the command
def createCommand(command, remote_path, placeholder, fname=False):

    # Output directory
    remote_path = os.path.join(remote_path,command.name)

    # If there are uneven number of files to placeholders
    if (command.cmd.count(placeholder) != len(command.files)):
        return 'Number of placeholders is not equal to the number of files given! Please check your command: {}'.format(command)

    # Command is sound, replace placeholders with files
    result = command.cmd[:]
    for f in command.files:
        pos = result.index(placeholder)
        remote = os.path.join(remote_path, os.path.basename(f))
        result[pos] = remote

    if fname:
        remote_path = os.path.join(remote_path, command.name + '_')
    # Try to replace the output if specified
    try:
        x = result.index('<output>')
        result[x] = remote_path
    except ValueError:
        pass
    
    # Return complete command
    return result

def submitJob(command):
    c = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o,e = c.communicate()
    jid = o.split()[2]
    return jid,o,e

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

def waitJob(jid, cluster='genesis'):
    r = checkJob(jid, cluster=cluster)
    while r:
        time.sleep(5)
        r = checkJob(jid, cluster=cluster)
    return True

def transfer(_file, dest):
    command = 'ssh apollo qsub /home/dmacmillan/scripts/bash/transfer_file.sh {} {}'.format(_file, dest)
    c = subprocess.Popen(command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    o,e = c.communicate()
#    print 'DONE'
    try:
        jid = o.split()[2]
    except Exception as e:
        print 'command: "{}"\no: "{}"\ne: "{}"'.format(command,o,e)
    print '({}) Transferring {} -> {}...'.format(jid, _file, dest)
    return command, jid, o, e

def generateQsub(command, exclusive=True, mem='3.83G', ncpus=12, queue='all.q', stdout=None, stderr=None):
    s = '#!/bin/bash\n'
    s += '#$ -S /bin/bash\n'
    s += '#$ -q {}\n'.format(queue)
    s += '#$ -pe ncpus {}\n'.format(ncpus)
    s += '#$ -l'
    if exclusive:
        s += ' excl=true,'
    else:
        s += ' '
    s += 'mem_free={},mem_token={},h_vmem={}\n'.format(mem,mem,mem)
    if stdout:
        s += '#$ -o {}\n'.format(stdout)
    if stderr:
        s += '#$ -e {}\n'.format(stderr)
    s += '#$ -V\n\n'
    s += command
    return s,stdout,stderr

def run(command, gen_path, indir, myncpus=1, mymem='3.83G', myfname=False):

    formatted_command = createCommand(command, gen_path, args.placeholder, fname=myfname)

    # Genesis directory to run job in
    gen_path = os.path.join(gen_path, command.name)

    # Create the directory on genesis
    if not os.path.exists(gen_path):
        print 'Creating genesis directory {}...'.format(gen_path)
        os.mkdir(gen_path)
        # Transfer all necessary files to gen_path on genesis
        transfers = []
        for i in command.files:
            c, jid, o, e = transfer(i, gen_path)
            transfers.append([c,jid])
        for i in transfers:
            waitJob(i[1], cluster='apollo')
#        print 'DONE'

    submit_path = os.path.join(gen_path, 'submit')

    # Generate qsub command
    qsub,o,e = generateQsub((' ').join(formatted_command), ncpus=myncpus, mem=mymem, stdout=os.path.join(gen_path,'{}.o'.format(command.name)), stderr=os.path.join(gen_path,'{}.e'.format(command.name)))

    # Write the command to genesis
    with open(submit_path, 'w') as f:
        f.write(qsub)
    #subprocess.call('ssh genesis echo -e \'{}\' > {}'.format(qsub, submit_path).split())

    # Command to submit the job on genesis
    #submit = 'ssh genesis qsub {}'.format(submit_path).split()
    submit = 'ssh genesis qsub {}'.format(submit_path).split()

    # Submit job on genesis
    jid,o,e = submitJob(submit)
    print 'Submitting job ({}) to Genesis...'.format(jid)
#    print 'DONE'
#    print 'job_id: {}'.format(jid)

    time.sleep(3)
    # Wait for the job to finish
    print 'Waiting for job to finish...'
    waitJob(jid)

    # Remove temporary files before transferring back
    for i in command.files:
        gen_file = os.path.join(gen_path, os.path.basename(i))
        try:
            os.remove(gen_file)
        except OSError:
            print 'Warning: Files are missing from this path? {}'.format(gen_path)

    # Transfer the completed job back to filer
    c,tid,o,e = transfer(gen_path, args.indir)
    waitJob(tid, cluster='apollo')

    # Clean genesis directory
    #print 'Keeping temporary files for debugging, uncomment line 197 to remove'
    print 'Removing leftover files...'
    shutil.rmtree(gen_path)
#    print 'DONE'

# Parse the commands
#parsed = parseCommands(args.commands)
parsed = parseConfig(args.config)

# Store number of commands to run
N = len(parsed)

# Display some information
print 'Number of jobs: {}'.format(N)

q = Queue()

#c, jid = transfer('/projects/dmacmillanprj2/polya/ccle/STAR/config', '/genesis/home/dmacmillan')
#logfile = open('./handler.log', 'w')
#logfile.write('{}\t{}\n'.format('job_name', 'command'))

themem = args.mem
thencpus = args.ncpus
thefname = args.filename

def worker(q, outdir, indir):
    for args in iter(q.get, None):
        try:
            run(args, outdir, indir, myncpus=thencpus, mymem=themem, myfname=thefname)
        except Exception as e:
            print traceback.format_exc()
        finally:
            q.task_done()

for com in parsed:
    q.put(com)

for i in range(args.queue_size):
    t = Thread(target = worker, args = (q, args.outdir, args.indir ))
    t.setDaemon(True)
    t.start()
    time.sleep(3)

q.join()

#run(parsed[0], '/genesis/extscratch/btl/dmacmillan/transfers', args.indir)
