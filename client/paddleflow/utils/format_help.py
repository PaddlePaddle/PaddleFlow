"""
Copyright (c) 2021 PaddlePaddle Authors. All Rights Reserve.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

#!/usr/bin/env python3
# -*- coding:utf8 -*-
import click

def command_required_option_from_option():
    """command option"""
    class CommandOptionRequiredClass(click.Command):
        """class command option"""
        def get_help(self, ctx):
            """help func"""
            orig_wrap_test = click.formatting.wrap_text

            def wrap_text(text, width=78, initial_indent='',
                          subsequent_indent='',
                          preserve_paragraphs=False):
                """
                wrap text
                """
                return orig_wrap_test(text.replace('\n', '\n\n'), width,
                                      initial_indent=initial_indent,
                                      subsequent_indent=subsequent_indent,
                                      preserve_paragraphs=True
                                      ).replace('\n\n', '\n')

            click.formatting.wrap_text = wrap_text
            return super(CommandOptionRequiredClass, self).get_help(ctx)

    return CommandOptionRequiredClass