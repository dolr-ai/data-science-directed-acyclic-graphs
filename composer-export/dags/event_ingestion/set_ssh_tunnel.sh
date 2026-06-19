# setup_root_ssh.sh - Quick SSH setup for GCP cluster master nodes

# Configuration - Replace with your actual SSH public key
SSH_PUBLIC_KEY="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDMnpqKt41K4u7sbPq7Cq+RqYphWZDhfM3kbGxUuPeybGjqJ3S+KYUPO6m7zzQA1ZHlhzjen/Zg/75NMFogQ95PJtNGt3OuWqzTNBChqii9VEeO14ViGuRJgoYaxsuYn0t4YEDpSBh08+GMn7YuQ92gbTGh7rRuxUG0VLumGlGn86Bb4/UnqgRnPzHp+i7kqQxjJMe3uLn3DeDlXBXa5+F4y9Fvzzl0YKHJapX3RyXltoscqpMgxc5HitdwbTcyTzT79S1Fd6egzLKtWASDpxGT2BbsJU8ShjID+9fZA8D1mjSwU92re6aSOb/ngagUpvFGLDvA9Ex+EKugFxZLm2rHrYz3cZ2GgeQ1k9yXPdKJofH+8YCV7CrGAyN/AoCKmaICjXTIkF2q+NvF/n2b93ZCfbCC8ENnpKvC7dXDGzWuyPcXXo4JhIK0HWyAf3YhVBox93P0vhZUE8H0PnzszhFgQ2Qw1SE7O4sbqKBAJ4zrXA7xlF7gX8SVMm7ov96Jrc3wjXVfOHDYPzCaYKf1iGJRQbimwPfpcI4tuyOToVuuU0476dYaeoiVue7wVpl3kTgyIZijru6f9m8S4QYxq6PN75+0954Ez/gBAteh1ycmj60nnCmKZYcYOvA/k1jlGzYMTWvesnnff7ipAS3ZfIsqtJsoNlHNwMHWymQAuBbxrQ== dopedhermit@Hermits-MacBook-Pro.local"

echo "=== Setting up SSH access for root user ==="

# Check if running as sudo/root
if [ "$EUID" -ne 0 ]; then
    echo "Error: Run with sudo"
    echo "Usage: sudo bash $0"
    exit 1
fi

# Check if SSH_PUBLIC_KEY is set
if [ -z "$SSH_PUBLIC_KEY" ]; then
    echo "Error: Set your SSH_PUBLIC_KEY variable first"
    exit 1
fi

echo "Creating SSH directory..."
mkdir -p /root/.ssh
chmod 700 /root/.ssh

echo "Adding SSH key..."
echo "$SSH_PUBLIC_KEY" >>/root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys

# Get external IP using GCP metadata
EXTERNAL_IP=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip)

# Get hostname for SSH config
HOSTNAME=$(hostname)

echo ""
echo "=== Setup Complete! ==="
echo "External IP: $EXTERNAL_IP"
echo "Hostname: $HOSTNAME"
echo ""
echo "SSH Command:"
echo "ssh root@$EXTERNAL_IP"
echo ""
echo "Cursor Connection:"
echo "root@$EXTERNAL_IP"
echo ""
echo "Add to ~/.ssh/config on your local machine:"
echo "Host $HOSTNAME"
echo "    HostName $EXTERNAL_IP"
echo "    User root"
echo "    Port 22"
echo "    IdentityFile ~/.ssh/id_ed25519"
echo "    ServerAliveInterval 60"
echo ""
echo "Then connect with: ssh $HOSTNAME"

