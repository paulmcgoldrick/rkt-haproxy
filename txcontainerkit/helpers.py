from subprocess import check_output


def get_interfaces():
    network = {}
    interfaces = check_output(
        """ip link | grep -e \'^\d\' | awk \'{ print $2 }\'"""
        """| sed  \'s/[@|:].*//g\'""", shell=True).split()
    print interfaces
    for interface in interfaces:
        if interface != "lo":
            network[interface] = check_output(
                """ip addr show dev {} | grep \"inet \""""
                """| awk \'{{ print $2 }}\'""".format(interface),
                shell=True).split('/')[0]
    return network
