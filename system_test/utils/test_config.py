
class TestConfig(object):

    @classmethod
    def get_test_user(self):
        return "hrt_qa"

    @classmethod
    def get_kinit_cmd(self, secureMode):
        if not secureMode:
            return "echo;"
        keytab = "/home/HDINSIGHT/hrt_qa/hadoopqa/keytabs/hrt_qa.headless.keytab"
        principle = "hrt_qa@HDINSIGHT.NET"
        kinit_cmd = "/usr/bin/kinit  -k -t %s %s;" % (keytab, principle)  # must end with ;
        return kinit_cmd

