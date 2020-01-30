#!/bin/bash
# properties = {properties}
if [[ -f ~/.bashrc &&  $(grep -c "__conda_setup=" ~/.bashrc) -gt 0 && $(grep -c "unset __conda_setup" ~/.bashrc) -gt 0 ]]; then
   echo "Sourcing .bashrc" 1>&2
   . ~/.bashrc
else
   echo "Exporting conda PATH" 1>&2
   export PATH=/ebio/abt3_projects/software/dev/miniconda3_dev/bin:$PATH
fi

{exec_job}
