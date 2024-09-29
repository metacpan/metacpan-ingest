package MetaCPAN::File;

use strict;
use warnings;
use v5.36;

use List::AllUtils qw< any >;
use Path::Tiny qw< path >;

use MetaCPAN::Logger qw< :log :dlog >;

use MetaCPAN::Ingest qw<
    extract_section
    strip_pod
>;

my @NOT_PERL_FILES = qw< SIGNATURE >;

my $RE_SECTION = qr/^\s*(\S+)((\h+-+\h+(.+))|(\r?\n\h*\r?\n\h*(.+)))?/ms;

sub new ( $class, %args ) {
    my $name    = $args{name}    or die "Missing name arg\n";
    my $release = $args{release} or die "Missing release arg\n";

    my $child = path($name);
    return if $class->_is_broken_file($name);

    my $relative = $child->relative( $release->{extract_dir} );
    my $stat     = do {
        my $s = $child->stat;
        +{ map { $_ => $s->$_ } qw< mode size mtime > };
    };
    return if ( $relative eq q{.} );

    my $directory = $child->is_dir;

    ( my $fpath = "$relative" ) =~ s/^.*?\///;
    my $filename = $fpath;
    $directory
        ? $filename =~ s/^(.*\/)?(.+?)\/?$/$2/
        : $filename =~ s/.*\///;
    $fpath = q{}
        if $relative !~ /\// && !$release->{archive}->is_impolite;

    my ( $dist, $metadata ) = @{$release}{qw< dist_info metadata >};

    my $self = DlogS_trace {"adding file $_"} +{
        author  => $dist->cpanid,
        binary  => -B $child,
        content => $directory ? "" : ( scalar $child->slurp ),
        date    => DateTime->from_epoch( epoch => $child->stat->mtime ) . "",
        directory    => $directory,
        distribution => $dist->dist,
        local_path   => $child . "",
        maturity     => $dist->maturity,
        metadata     => $metadata->as_struct,
        modules      => [],
        name         => $filename,
        path         => $fpath,
        download_url => $args{download_url},
        release      => $dist->distvname,
        stat         => $stat,
        status       => $release->{status},
        version      => $release->{version},
    };

    bless $self, $class;

    # need the metadata object, not the struct
    $self->{indexed} = $self->_should_index_file($metadata);
    $self->_set_mime();

    return $self;
}

sub as_struct ($self) {
    return +{ map { $_ => $self->{$_} } keys %$self };
}

sub _is_broken_file ( $self, $name ) {
    return 1 if ( -p $name || !-e $name );

    if ( -l $name ) {
        my $syml = readlink $name;
        return 1 if ( !-e $name && !-l $name );
    }

    return 0;
}

sub add_module ( $self, $module ) {
    push @{ $self->{modules} }, $module;
}

sub _should_index_file ( $self, $metadata ) {
    return 0 if !$metadata->should_index_file( $self->{path} );

    # files listed under 'other files' are not shown in a search
    return 0 if $self->_is_in_other_files();

    # files under no_index directories should not be indexed
    return 0
        if grep { $self->{path} eq $_ or $self->{path} =~ m|^$_/| }
        @{ $metadata->no_index->{directory} };

    return 1;
}

sub _set_mime ($self) {
    my $mime;

    if (  !$self->{directory}
        && $self->{name} !~ /\./
        && grep { $self->{name} ne $_ } @NOT_PERL_FILES )
    {
        $mime = "text/x-script.perl" if ( $self->{content} =~ /^#!.*?perl/ );
    }
    else {
        $mime = Plack::MIME->mime_type( $self->{name} ) || 'text/plain';
    }

    $self->{mime} = $mime;
}

sub _is_in_other_files ($self) {
    my @other = qw<
        AUTHORS
        Build.PL
        Changelog
        ChangeLog
        CHANGELOG
        Changes
        CHANGES
        CONTRIBUTING
        CONTRIBUTING.md
        CONTRIBUTING.pod
        Copying
        COPYRIGHT
        cpanfile
        CREDITS
        dist.ini
        FAQ
        INSTALL
        INSTALL.md
        INSTALL.pod
        LICENSE
        Makefile.PL
        MANIFEST
        META.json
        META.yml
        NEWS
        README
        README.md
        README.pod
        THANKS
        Todo
        ToDo
        TODO
    >;

    return any { $self->{name} eq $_ } @other;
}

sub full_path ($self) {
    return join( '/', @{$self}{qw< author release path >} );
}

=head2 is_perl_file

Return true if the file extension is one of C<pl>, C<pm>, C<pod>, C<t>
or if the file has no extension, is not a binary file and its size is less
than 131072 bytes. This is an arbitrary limit but it keeps the pod parser
happy and the indexer fast.

=cut

sub _is_perl_file ($self) {
    return 0 if ( $self->{directory} );
    return 1 if ( $self->{name} =~ /\.(pl|pm|pod|t)$/i );
    return 1 if ( $self->{mime} and $self->{mime} eq "text/x-script.perl" );
    return 1
        if ( $self->{name} !~ /\./
        && !( grep { $self->{name} eq $_ } @NOT_PERL_FILES )
        && !$self->{binary}
        && $self->{stat}{size} < 2**17 );
    return 0;
}

sub _section ($self) {
    my $section = extract_section( $self->{content}, 'NAME' );

    # if it's a POD file without a name section, let's try to generate
    # an abstract and name based on filename
    if ( !$section && $self->{path} =~ /\.pod$/ ) {
        $section = $self->{path};
        $section =~ s{^(lib|pod|docs)/}{};
        $section =~ s{\.pod$}{};
        $section =~ s{/}{::}g;
    }

    return undef unless ($section);
    $section =~ s/^=\w+.*$//mg;
    $section =~ s/X<.*?>//mg;

    return $section;
}

=head2 documentation

Holds the name for the documentation in this file.

If the file L<is a pod file|/is_pod_file>, the name is derived from the
C<NAME> section. If the file L<is a perl file|/is_perl_file> and the
name from the C<NAME> section matches one of the modules in L</module>,
it returns the name. Otherwise it returns the name of the first module
in L</module>. If there are no modules in the file the documentation is
set to C<undef>.

=cut

sub add_documentation ($self) {
    return undef unless $self->_is_perl_file();

    my $section = $self->_section();
    return undef unless $section;

    my $doc;
    my $val;

    if ( $section =~ $RE_SECTION ) {
        my $name = strip_pod($1);
        $doc = $name if $name =~ /^[\w\.:\-_']+$/;
    }

    return undef unless length $doc;

    # Modules to be indexed
    my @indexed = grep { $_->{indexed} } @{ $self->{modules} };

    # This is a Pod file, return its name
    $val = $doc
        if !$val
        and $doc
        and $self->_is_perl_file();

    # OR: found an indexed module with the same name
    $val = $doc
        if !$val
        and $doc
        and grep { $_->{name} eq $doc } @indexed;

    # OR: found an indexed module with a name
    if ( !$val and my ($mod) = grep { defined $_->{name} } @indexed ) {
        $val = $mod->{name};
    }

    # OR: we have a parsed documentation
    $val = $doc if !$val and defined $doc;

    # OR: found ANY module with a name (better than nothing)
    if ( !$val
        and my ($mod) = grep { defined $_->{name} } @{ $self->{modules} } )
    {
        return $mod->{name};
    }

    $self->{documentation}        = $val;
    $self->{documentation_length} = length($val);

    return undef;
}

sub _is_pod_file ($self) {
    $self->{name} =~ /\.pod$/i;
}

=head2 suggest

Autocomplete info for documentation.

=cut

sub set_suggest ($self) {
    my $doc = $self->{documentation};
    return "" unless $doc;

    my $weight = 1000 - length($doc);
    $weight = 0 if $weight < 0;

    $self->{suggest} = +{
        input   => [$doc],
        payload => { doc_name => $doc },
        weight  => $weight,
    };
}

sub set_deprecated ( $self, $value ) {
    $self->{deprecated} = $value;
}

sub empty_modules ($self) {
    $self->{modules} = [];
}

=head2 set_authorized

Expects a C<$perms> parameter which is a HashRef. The key is the module name
and the value an ArrayRef of author names who are allowed to release
that module.

The method returns a list of unauthorized, but indexed modules.

Unauthorized modules are modules that were uploaded in the name of a
different author than stated in the C<06perms.txt.gz> file. One problem
with this file is, that it doesn't record historical data. It may very
well be that an author was authorized to upload a module at the time.
But then his co-maintainer rights might have been revoked, making consecutive
uploads of that release unauthorized. However, since this script runs
with the latest version of C<06perms.txt.gz>, the former upload will
be flagged as unauthorized as well. Same holds the other way round,
a previously unauthorized release would be flagged authorized if the
co-maintainership was added later on.

If a release contains unauthorized modules, the whole release is marked
as unauthorized as well.

=cut

sub set_authorized ( $self, $perms ) {

    # only authorized perl distributions make it into the CPAN
    return () if ( $self->{distribution} eq 'perl' );

    foreach my $module ( @{ $self->{modules} } ) {
        my $name = $module->{name};
        if ( $perms->{$name}
            && !grep { $_ eq $self->{author} } @{ $perms->{$name} } )
        {
            $module->{authorized} = 0;
        }
    }

    my $doc = $self->{documentation};
    if (   $self->{authorized}
        && $doc
        && $perms->{$doc}
        && !grep { $_ eq $self->{author} } @{ $perms->{$doc} } )
    {
        $self->{authorized} = 0;
    }

    return grep { !$_->{authorized} && $_->{indexed} } @{ $self->{modules} };
}

1;

__END__
