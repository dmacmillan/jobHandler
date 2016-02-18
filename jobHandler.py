import argparse, os, sys, subprocess, re, time, random, string, shutil
from Queue import Queue
from threading import Thread

parser = argparse.ArgumentParser(description='Manage a number of tasks requiring transfer of files and submission to genesis, movement of important files back to filer, and garbage removal.')

parser.add_argument('config', help='A configuration file with the following format. Each line contains 3 columns separated by tabs, column 1 is the name of the job (must be unique!). Column 2 is the command itself, but with local files using by the command replaced by the "--placeholder" character. If there is a flag to specify an output directory, use "<output>" instead of the directory. Column 3 contains a comma-separated list of the files required in order of appearance in the command.')
parser.add_argument('outdir', help='Directory to output to on the cluster')
parser.add_argument('indir', help='The local directory to store completed jobs in')
parser.add_argument('-p', '--placeholder', default='?', help='The character used to indicate a file within a command. Default is "?"')
parser.add_argument('-qs', '--queue_size', type=int, default=20, help='The number of commands to be running concurrently. Default is 20.')
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
def createCommand(command, remote_path, placeholder):

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
    sprint('Transferring {} -> {}...'.format(_file, dest))
    o,e = c.communicate()
    print 'DONE'
    jid = o.split()[2]
    return command, jid, o, e

def generateQsub(command, mem='3.83G', ncpus=12, queue='all.q', stdout=None, stderr=None):
    s = '#!/bin/bash\n'
    s += '#$ -S /bin/bash\n'
    s += '#$ -q {}\n'.format(queue)
    s += '#$ -pe ncpus {}\n'.format(ncpus)
    s += '#$ -l excl=true,mem_free={},mem_token={},h_vmem={}\n'.format(mem,mem,mem)
    s += '#$ -j y\n'
    s += '#$ -V\n\n'
    s += command
    if stdout:
        s += ' > {}'.format(stdout)
    if stderr:
        s += ' 2> {}'.format(stderr)
    return s,stdout,stderr

def run(command, gen_path, indir):

    # Create a random name for the job
    #job_name = ''.join(random.choice(string.ascii_lowercase +  string.digits) for _ in range(20))

    # Genesis directory to run job in
    #gen_path = os.path.join(gen_path, job_name)

    formatted_command = createCommand(command, gen_path, args.placeholder)
    #print formatted_command
    #sys.exit()

    # Genesis directory to run job in
    gen_path = os.path.join(gen_path, command.name)

    # Create the directory on genesis
    if not os.path.exists(gen_path):
        sprint('Creating genesis directory {}...'.format(gen_path))
        os.mkdir(gen_path)
        # Transfer all necessary files to gen_path on genesis
        transfers = []
        for i in command.files:
            c, jid, o, e = transfer(i, gen_path)
            transfers.append([c,jid])
        for i in transfers:
            waitJob(i[1], cluster='apollo')
        print 'DONE'

    # Record this name in a logfile for future reference
    #logfile.write('{}\t{}\n'.format(job_name, ' '.join(command[0])))
    #logfile.flush()


    # Alter the command to include the correct genesis paths
#    i = 1
#    while True:
#        try:
#            x = command[0].index('?')
#            command[0][x] = os.path.join(gen_path, os.path.basename(command[i]))
#            i += 1
#        except ValueError:
#            break

    submit_path = os.path.join(gen_path, 'submit')

    # Generate qsub command
    qsub,o,e = generateQsub((' ').join(formatted_command), ncpus=1)

    # Write the command to genesis
    #subprocess.call('ssh genesis echo \'{}\' > {}'.format((' ').join(command[0]), submit_path).split())
    with open(submit_path, 'w') as f:
        f.write(qsub)
    #subprocess.call('ssh genesis echo -e \'{}\' > {}'.format(qsub, submit_path).split())

    # Command to submit the job on genesis
    #submit = 'ssh genesis qsub {}'.format(submit_path).split()
    submit = 'ssh genesis qsub {}'.format(submit_path).split()

    # Submit job on genesis
    sprint('Submitting job to Genesis...')
    jid,o,e = submitJob(submit)
    print 'DONE'
    print 'job_id: {}'.format(jid)

    # Wait for the job to finish
    sprint('Waiting for job to finish...')
    waitJob(jid)
    print 'DONE'

    # Transfer the completed job back to filer
    c,tid,o,e = transfer(gen_path, args.indir)
    waitJob(tid, cluster='apollo')

    # Clean genesis directory
    sprint('Removing leftover files...')
    shutil.rmtree(gen_path)
    print 'DONE'

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

run(parsed[0], '/genesis/extscratch/btl/dmacmillan/transfers', args.indir)
