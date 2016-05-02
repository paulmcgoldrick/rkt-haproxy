from jinja2 import Template
from jinja2 import Environment
from jinja2 import meta


class ConfigurationTemplate(object):

    def __init__(self, template):
        """
        Use Jinja2's meta functionality
        to determine what context is needed to
        render this command
        """
        with open(template) as tmpl:
            tmpl_txt = tmpl.read()
            self.template = Template(tmpl_txt)
            e = Environment()
            self.needed = meta.find_undeclared_variables(
                e.parse(tmpl_txt))

    def render(self, cxt):
        """
        render this template for the context object provided
        """
        return self.template.render(cxt)


class CommandTemplate(object):

    def __init__(self, command):
        if isinstance(command, int):
            self.template = command
            self.signal = True
        else:
            self.signal = False
            e = Environment()
            self.needed = meta.find_undeclared_variables(e.parse(command))
            print "NEEDED IS ", self.needed
            self.template = Template(command)

    def render(self, ctx):
        """
        render this template for the context object provided
        """
        print 'template is ', self.template
        print 'ctx is ', ctx
        if self.signal:
            return self.template
        else:
            return self.template.render(ctx)
