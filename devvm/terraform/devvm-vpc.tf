resource "google_compute_network" "devvm_vpc" {
  auto_create_subnetworks = true
  mtu                     = 1460
  name                    = "devvm-vpc"
  project                 = var.project_id
  routing_mode            = "REGIONAL"
}

resource "google_compute_firewall" "devvm_vpc_allow_custom" {
  allow {
    ports    = ["6000"]
    protocol = "tcp"
  }

  allow {
    ports    = ["51820"]
    protocol = "udp"
  }

  description   = "Allows connection from any source to any instance on the network using custom protocols."
  direction     = "INGRESS"
  name          = "devvm-vpc-allow-custom"
  network       = google_compute_network.devvm_vpc.self_link
  priority      = 65534
  project       = var.project_id
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "devvm_vpc_allow_icmp" {
  allow {
    protocol = "icmp"
  }

  description   = "Allows ICMP connections from any source to any instance on the network."
  direction     = "INGRESS"
  name          = "devvm-vpc-allow-icmp"
  network       = google_compute_network.devvm_vpc.self_link
  priority      = 65534
  project       = var.project_id
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "devvm_vpc_allow_ssh" {
  allow {
    ports    = ["22"]
    protocol = "tcp"
  }

  description   = "Allows TCP connections from any source to any instance on the network using port 22."
  direction     = "INGRESS"
  name          = "devvm-vpc-allow-ssh"
  network       = google_compute_network.devvm_vpc.self_link
  priority      = 65534
  project       = var.project_id
  source_ranges = ["0.0.0.0/0"]
}
