packer {
  required_plugins {
    ansible = {
      version = "~> 1"
      source  = "github.com/hashicorp/ansible"
    }
    googlecompute = {
      source  = "github.com/hashicorp/googlecompute"
      version = "~> 1"
    }
  }
}

source "googlecompute" "gce" {
  disk_size               = 70
  disk_type               = "pd-balanced"
  image_family            = "${var.image_family}-devvm"
  image_name              = "devvm-${var.image_tag}"
  image_storage_locations = ["eu"]
  machine_type            = "${var.instance_type}"
  metadata = {
    enable-oslogin = "True"
  }
  labels = {
    "devvm" = "true"
  }
  network                 = "devvm-vpc"
  project_id              = "${var.project_id}"
  source_image_family     = "${var.image_family}"
  use_os_login            = true
  zone                    = "${var.zone}"
  credentials_file        = "${var.account_file}"
  ssh_username            = "ubuntu"
}

build {
  sources = ["source.googlecompute.gce"]

  provisioner "ansible" {
    command        = "/root/.local/bin/ansible-playbook"
    galaxy_command = "/root/.local/bin/ansible-galaxy"
    ansible_env_vars = [
      "ANSIBLE_HOST_KEY_CHECKING=False",
      "ANSIBLE_PIPELINING=true",
      "ANSIBLE_NOCOLOR=True",
      "ANSIBLE_DEBUG=false",
    ]
    playbook_file = "${path.root}/../ansible/site.yml"
    user          = "${build.User}"
    # Precedence rules force specifying these as extra vars. We have also vars
    # defined in group vars file that are used when running locally using
    # os-login.
    # NOTE(prozlach): WARNING - SCP became deprecated with OpenSSL 9+, we need
    # to explicitly enable it with `-O` option for cloud-login to work. In the next
    # bump of the base CI image, we will probably need to find a different way to
    # copy files. See also https://github.com/ansible/ansible/issues/78600
    extra_arguments = [
      "--extra-vars",
      "ansible_ssh_args='-o ControlMaster=auto -o ControlPersist=60s -o HostkeyAlgorithms=+ssh-rsa -oPubkeyAcceptedKeyTypes=+ssh-rsa -o PreferredAuthentications=publickey -o KbdInteractiveAuthentication=no -o PasswordAuthentication=no -o ConnectTimeout=20' ansible_scp_extra_args='-O -o ControlMaster=auto -o ControlPersist=60s -o HostkeyAlgorithms=+ssh-rsa -oPubkeyAcceptedKeyTypes=+ssh-rsa -o PreferredAuthentications=publickey -o KbdInteractiveAuthentication=no -o PasswordAuthentication=no -o ConnectTimeout=20'",
    ]
    galaxy_file = "${path.root}/../ansible/requirements.yml"
  }
}
