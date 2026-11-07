"""Installation fixtures use temporary homes and never invoke network pip."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('cpr_install', ROOT / 'scripts/install.py')
installer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(installer)


class InstallTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='cpr-install-fixture-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / 'source'
        self.codex, self.claude = self.root / 'codex', self.root / 'claude'
        self.source.mkdir()
        for relative in installer.FILES:
            path = self.source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            if relative.startswith('scripts/'):
                shutil.copy2(ROOT / relative, path)
            else:
                path.write_text('fixture\n', encoding='utf-8')
        (self.source / 'pyproject.toml').write_text('[project]\nname="fixture"\ndependencies=["websockets>=14,<17", "wsproto>=1.2,<2", "psutil>=6,<8"]\n')
        package = self.source / 'src/codex_provider_runtime'
        package.mkdir(parents=True)
        (package / '__init__.py').write_text('')
        (package / 'cli.py').write_text('import json\ndef main():\n print(json.dumps({"fixture": True}))\n return 0\n')
        for name in installer.CODEX_SKILLS:
            path = self.source / 'skills' / name
            path.mkdir(parents=True)
            (path / 'SKILL.md').write_text('fixture skill')
        for relative in ['.git/private', '.venv/private', 'experiments/private', 'tests/private', 'docs/private.md', 'src/codex_provider_runtime/__pycache__/cached.pyc']:
            path = self.source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('must not ship')
        for home in [self.codex, self.claude]:
            home.mkdir()
            (home / 'config.toml').write_text('fixture = "preserve"\n')

    def install(self, **kwargs):
        return installer.install(self.source, self.codex, self.claude, skip_deps=True, **kwargs)

    def destination(self, home):
        return home / 'tool-sources' / installer.NAME

    def test_both_real_trees_relative_skills_and_no_private_content(self):
        result = self.install()
        self.assertEqual(result['dependencies'], 'skipped')
        for home in [self.codex, self.claude]:
            destination = self.destination(home)
            self.assertTrue(destination.is_dir())
            self.assertFalse(destination.is_symlink())
            self.assertEqual(sorted(p.name for p in (destination / 'docs').iterdir()), ['usage.md'])
            for excluded in ['.git', '.venv', 'experiments', 'tests', 'src/codex_provider_runtime/__pycache__']:
                self.assertFalse((destination / excluded).exists())
            self.assertEqual((home / 'config.toml').read_text(), 'fixture = "preserve"\n')
        self.assertEqual(sorted(p.name for p in (self.codex / 'skills').iterdir()), sorted(installer.CODEX_SKILLS))
        self.assertEqual(sorted(p.name for p in (self.claude / 'skills').iterdir()), ['codex-provider-external'])
        for home, names in [(self.codex, installer.CODEX_SKILLS), (self.claude, ('codex-provider-external',))]:
            for name in names:
                link = home / 'skills' / name
                self.assertEqual(link.resolve(), self.destination(home) / 'skills' / name)
                if link.is_symlink():
                    self.assertFalse(os.path.isabs(os.readlink(link)))

    def test_installed_launcher_survives_source_removal_and_vendors_path(self):
        (self.source / 'src/codex_provider_runtime/cli.py').write_text('import json\nimport fixture_dependency\ndef main():\n print(json.dumps({"dependency": fixture_dependency.VALUE}))\n return 0\n')
        def dependencies(stage, python):
            deps = stage / 'runtime-deps'
            deps.mkdir()
            (deps / 'fixture_dependency.py').write_text('VALUE = "vendored"\n')
        with patch.object(installer, 'install_dependencies', side_effect=dependencies) as vendor:
            installer.install(self.source, self.codex, self.claude)
        self.assertEqual(vendor.call_count, 1)
        shutil.rmtree(self.source)
        for home in [self.codex, self.claude]:
            result = subprocess.run([sys.executable, '-I', '-B', str(self.destination(home) / 'scripts/run.py')], cwd=self.root, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), {'dependency': 'vendored'})

    def test_update_retains_prior_tree_and_uses_same_skill_links(self):
        self.install()
        old_link = os.readlink(self.codex / 'skills/codex-provider-tasks') if os.name != 'nt' else None
        (self.destination(self.codex) / 'old-only.txt').write_text('prior version')
        (self.source / 'README.md').write_text('new version')
        result = self.install()
        codex = next(item for item in result['installed'] if item['target'] == 'codex')
        self.assertEqual((self.destination(self.codex) / 'README.md').read_text(), 'new version')
        self.assertEqual((Path(codex['backup']) / 'old-only.txt').read_text(), 'prior version')
        self.assertFalse((self.destination(self.codex) / 'old-only.txt').exists())
        if old_link:
            self.assertEqual(os.readlink(self.codex / 'skills/codex-provider-tasks'), old_link)

    def test_failed_dependencies_leave_both_existing_trees_intact(self):
        self.install()
        with patch.object(installer, 'install_dependencies', side_effect=installer.InstallError('fixture pip failure')):
            with self.assertRaisesRegex(installer.InstallError, 'pip failure'):
                installer.install(self.source, self.codex, self.claude)
        for home in [self.codex, self.claude]:
            self.assertEqual((self.destination(home) / 'README.md').read_text(), 'fixture\n')
            self.assertFalse(list((home / 'tool-sources').glob('.cpr-stage-*')))
            self.assertFalse(list((home / 'tool-sources').glob('*.lock')))

    def test_failed_link_activation_rolls_back_both_trees(self):
        self.install()
        (self.source / 'README.md').write_text('candidate')
        with patch.object(installer, 'link_skill', side_effect=installer.InstallError('fixture link failure')):
            with self.assertRaisesRegex(installer.InstallError, 'link failure'):
                self.install()
        for home in [self.codex, self.claude]:
            self.assertEqual((self.destination(home) / 'README.md').read_text(), 'fixture\n')
            self.assertTrue((home / 'skills/codex-provider-external/SKILL.md').is_file())

    def test_failed_directory_switch_restores_old_tree(self):
        self.install(target='codex')
        replace = installer.os.replace
        fired = False
        def fail_switch(source, target):
            nonlocal fired
            if not fired and Path(source).name.startswith('.cpr-stage-') and Path(target) == self.destination(self.codex):
                fired = True
                raise OSError('fixture switch failure')
            return replace(source, target)
        with patch.object(installer.os, 'replace', side_effect=fail_switch):
            with self.assertRaisesRegex(OSError, 'switch failure'):
                self.install(target='codex')
        self.assertEqual((self.destination(self.codex) / 'README.md').read_text(), 'fixture\n')

    @unittest.skipIf(os.name == 'nt', 'Source symlink fixture requires symlink privileges')
    def test_source_symlink_cannot_pull_external_private_content(self):
        private = self.root / 'private'
        private.mkdir()
        (private / 'secret.py').write_text('private fixture')
        package = self.source / 'src/codex_provider_runtime'
        package.rename(self.source / 'old-package')
        package.symlink_to(private, target_is_directory=True)
        with self.assertRaisesRegex(installer.InstallError, 'symlinks'):
            self.install()
        self.assertFalse(self.destination(self.codex).exists())

    @unittest.skipIf(os.name == 'nt', 'Source symlink fixture requires symlink privileges')
    def test_symlinked_source_parent_cannot_escape_allowlist(self):
        private = self.root / 'external-docs'
        private.mkdir()
        (private / 'usage.md').write_text('private fixture')
        shutil.rmtree(self.source / 'docs')
        (self.source / 'docs').symlink_to(private, target_is_directory=True)
        with self.assertRaisesRegex(installer.InstallError, 'symlinks'):
            self.install()
        self.assertFalse(self.destination(self.codex).exists())

    def test_unrelated_skill_directory_is_preserved(self):
        skill = self.codex / 'skills/codex-provider-external'
        skill.mkdir(parents=True)
        (skill / 'SKILL.md').write_text('user-owned')
        with self.assertRaisesRegex(installer.InstallError, 'unrelated skill'):
            self.install()
        self.assertEqual((skill / 'SKILL.md').read_text(), 'user-owned')
        self.assertFalse(self.destination(self.codex).exists())

    def test_single_target_does_not_touch_other_home(self):
        self.install(target='claude')
        self.assertFalse((self.codex / 'tool-sources').exists())
        self.assertTrue(self.destination(self.claude).is_dir())

    def test_dependency_command_vendors_and_redacts_pip_failure(self):
        stage = self.root / 'stage'
        stage.mkdir()
        shutil.copy2(self.source / 'pyproject.toml', stage / 'pyproject.toml')
        secret = 'fixture-private-index-value'
        with patch.object(installer.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', secret)) as run:
            with self.assertRaises(installer.InstallError) as caught:
                installer.install_dependencies(stage, sys.executable)
        command = run.call_args.args[0]
        self.assertIn('--target', command)
        self.assertIn(str(stage / 'runtime-deps'), command)
        self.assertNotIn(secret, str(caught.exception))


if __name__ == '__main__':
    unittest.main()
