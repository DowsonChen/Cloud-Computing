# ~/.bashrc: executed by bash(1) for non-login shells.
# see /usr/share/doc/bash/examples/startup-files (in the package bash-doc)
# for examples

# If not running interactively, don't do anything
[ -z "$PS1" ] && return

# make less more friendly for non-text input files, see lesspipe(1)
[ -x /usr/bin/lesspipe ] && eval "$(SHELL=/bin/sh lesspipe)"

# set variable identifying the chroot you work in (used in the prompt below)
if [ -z "$debian_chroot" ] && [ -r /etc/debian_chroot ]; then
    debian_chroot=$(cat /etc/debian_chroot)
fi

# set a fancy prompt (non-color, unless we know we "want" color)
case "$TERM" in
    xterm-color) color_prompt=yes;;
esac


# If this is an xterm set the title to user@host:dir
case "$TERM" in
xterm*|rxvt*)
    PS1="\[\e]0;${debian_chroot:+($debian_chroot)}\u@\h: \w\a\]$PS1"
    ;;
*)
    ;;
esac

# enable color support of ls and also add handy aliases
if [ -x /usr/bin/dircolors ]; then
    test -r ~/.dircolors && eval "$(dircolors -b ~/.dircolors)" || eval "$(dircolors -b)"
    alias ls='ls --color=auto'
    #alias dir='dir --color=auto'
    #alias vdir='vdir --color=auto'

    alias grep='grep --color=auto'
    alias fgrep='fgrep --color=auto'
    alias egrep='egrep --color=auto'
fi

# some more ls aliases
alias ll='ls -alF'
alias la='ls -A'
alias l='ls -CF'

# Add an "alert" alias for long running commands.  Use like so:
#   sleep 10; alert
alias alert='notify-send --urgency=low -i "$([ $? = 0 ] && echo terminal || echo error)" "$(history|tail -n1|sed -e '\''s/^\s*[0-9]\+\s*//;s/[;&|]\s*alert$//'\'')"'

# Alias definitions.
# You may want to put all your additions into a separate file like
# ~/.bash_aliases, instead of adding them here directly.
# See /usr/share/doc/bash-doc/examples in the bash-doc package.

if [ -f ~/.bash_aliases ]; then
    . ~/.bash_aliases
fi

# enable programmable completion features (you don't need to enable
# this, if it's already enabled in /etc/bash.bashrc and /etc/profile
# sources /etc/bash.bashrc).
if [ -f /etc/bash_completion ] && ! shopt -oq posix; then
    . /etc/bash_completion
fi

## added by JMB for 15-319
# NOTE to students - do not remove this logging mechanism
#  otherwise we can't verify your work and your project
#  grades will be affected (0 points).  This doesn't log
#  keystrokes, simply each command that is run, and parameters.
if [ -f /etc/bash_logger ]
then
  source /etc/bash_logger
fi


##############################################################################

#insert your keys below
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=

#give warning to users (skip for proj2)
# : ${AWS_ACCESS_KEY_ID:?"Edit your .bashrc to set this variable"}
# : ${AWS_SECRET_ACCESS_KEY:?"Edit your .bashrc to set this variable"}

export WHIRR_HOME=$HOME/whirr-0.8.1
export WHIRR_IDENTITY=$AWS_ACCESS_KEY_ID
export WHIRR_CREDENTIAL=$AWS_SECRET_ACCESS_KEY

export HADOOP_PREFIX=$HOME/hadoop-1.0.4
 export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_73/

export ELASTIC_MAPREDUCE_ACCESS_ID=$AWS_ACCESS_KEY_ID
export ELASTIC_MAPREDUCE_PRIVATE_KEY=$AWS_SECRET_ACCESS_KEY
export ELASTIC_MAPREDUCE_KEY_PAIR="<insert the name of your Amazon ec2 key-pair here>"
export ELASTIC_MAPREDUCE_KEY_PAIR_FILE="<insert the path to the .pem file for your Amazon ec2 key pair here>"
export ELASTIC_MAPREDUCE_REGION="us-east-1"
#export ELASTIC_MAPREDUCE_ENABLE_DEBUGGING
#export ELASTIC_MAPREDUCE_LOG_URI

export PATH=$PATH:$WHIRR_HOME/bin:$HADOOP_PREFIX/bin:$HOME/elastic-mapreduce

